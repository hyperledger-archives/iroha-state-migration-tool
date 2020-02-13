#!/usr/bin/env python3

import argparse
import logging
import os
import psycopg2
import typing
import voluptuous

NAME = 'SchemaMigrationTool'
DEFAULT_TRANSITIONS_DIR = 'migration_data'
LOGGER = logging.getLogger(NAME)
MIGRATION_DOCS_URL = 'https://iroha.readthedocs.io/en/1.1.2/maintenance/restarting_node.html#state-database-schema-version'

VERSION_DICT_SCHEMA = {
    'iroha_major': int,
    'iroha_minor': int,
    'iroha_patch': int
}
VERSION_SCHEMA = voluptuous.Any(VERSION_DICT_SCHEMA,
                                (int, ) * len(VERSION_DICT_SCHEMA))
TRANSITION_SCHEMA = voluptuous.Schema({
    'from': VERSION_SCHEMA,
    'to': VERSION_SCHEMA,
    'function': callable
})


class UserParam:
    def __init__(self,
                 cmd_line_arg,
                 env_key,
                 descr,
                 transformer=None,
                 default=None):
        self.cmd_line_arg = cmd_line_arg
        self.env_key = env_key
        self.descr = descr
        self.transformer = transformer
        self.default = default


def check_nonempty_string(val: str) -> str:
    assert len(val) > 0
    return val


def check_convert_nonnegative_int(val: str) -> int:
    val = int(val)
    assert val >= 0
    return val


PARAMS = {
    'pg_ip':
    UserParam('pg_ip', 'IROHA_POSTGRES_HOST',
              'PostgreSQL WSV database IP address.', check_nonempty_string),
    'pg_port':
    UserParam('pg_port', 'IROHA_POSTGRES_PORT',
              'PostgreSQL WSV database port.', check_convert_nonnegative_int,
              5432),
    'pg_user':
    UserParam('pg_user', 'IROHA_POSTGRES_USER',
              'PostgreSQL WSV database username.', check_nonempty_string),
    'pg_password':
    UserParam('pg_password', 'IROHA_POSTGRES_PASSWORD',
              'PostgreSQL WSV database password.'),
    'pg_dbname':
    UserParam('pg_dbname', 'IROHA_POSTGRES_DBNAME',
              'PostgreSQL WSV database name.', check_nonempty_string),
    'iroha_version_major':
    UserParam('iroha_version_major', 'IROHA_VERSION_MAJOR',
              'Target Iroha binary version major number',
              check_convert_nonnegative_int),
    'iroha_version_minor':
    UserParam('iroha_version_minor', 'IROHA_VERSION_MINOR',
              'Target Iroha binary version minor number',
              check_convert_nonnegative_int),
    'iroha_version_patch':
    UserParam('iroha_version_patch', 'IROHA_VERSION_PATCH',
              'Target Iroha binary version patch number',
              check_convert_nonnegative_int),
}


class SchemaVersion:
    def __init__(self, iroha_major, iroha_minor, iroha_patch):
        self.iroha_major = iroha_major
        self.iroha_minor = iroha_minor
        self.iroha_patch = iroha_patch

    def toString(self):
        return str(self.__dict__)

    def toShortString(self):
        return '.'.join(map(str, (self.iroha_major, self.iroha_minor, self.iroha_patch)))

    def __eq__(self, rhs):
        return self.__dict__ == rhs.__dict__

    def __repr__(self):
        return 'SchemaVersion: {}'.format(self.toShortString())


def get_params(args: argparse.Namespace) -> None:
    """Substitutes PARAMS values with user provided data."""
    def get_raw(param_name):
        param = PARAMS[param_name]
        if hasattr(args, param_name):
            val = getattr(args, param_name)
            if val is not None:
                return val
        if param.env_key in os.environ:
            return os.environ[param.env_key]
        print('You have not specified {} '\
            'You can set it via command line key {} '\
            'or environment variable {}. '\
            'Alternatively, you can type the value here: '.format(
                param.descr,
                param.cmd_line_arg,
                param.env_key),
            end='')
        return input()

    def get_transformed(param_name):
        transformer = PARAMS[param_name].transformer
        raw_val = get_raw(param_name)
        return raw_val if transformer is None else transformer(raw_val)

    for param_name in PARAMS:
        PARAMS[param_name] = get_transformed(param_name)
        LOGGER.debug('Using {} = {}'.format(param_name, PARAMS[param_name]))


def get_current_db_version(connection):
    cur = connection.cursor()
    try:
        cur.execute('select iroha_major, iroha_minor, iroha_patch from schema_version;')
        version_data = cur.fetchall()
        LOGGER.debug('Fetched version data from DB: {}'.format(version_data))
        assert len(version_data) == 1
        return SchemaVersion(*version_data[0])
    except Exception as e:
        LOGGER.warning(
            'Could not read database schema version information: {}'.format(e))
        return None


def force_schema_version(connection, schema_version: SchemaVersion) -> None:
    LOGGER.info('Setting schema version to {}'.format(schema_version.toShortString()))
    cur = connection.cursor()
    cur.execute("""
        create table if not exists schema_version (
            lock char(1) default 'X' not null primary key,
            iroha_major int not null,
            iroha_minor int not null,
            iroha_patch int not null
        );
        """)
    cur.execute(
        """
        insert into schema_version (iroha_major, iroha_minor, iroha_patch)
        values (
            %(iroha_major)s,
            %(iroha_minor)s,
            %(iroha_patch)s
        )
        on conflict (lock) do update set
            iroha_major = %(iroha_major)s,
            iroha_minor = %(iroha_minor)s,
            iroha_patch = %(iroha_patch)s
        """, schema_version.__dict__)
    connection.commit()


class Transition:
    def __init__(self, from_version: SchemaVersion, to_version: SchemaVersion,
                 function: typing.Callable):
        self.from_version = from_version
        self.to_version = to_version
        self.function = function

    def __repr__(self):
        return 'Transition: {} -> {}'.format(self.from_version.toShortString(),
                                             self.to_version.toShortString())


TRANSITIONS = list()


def load_transitions_from_dir(path: str) -> None:
    import glob
    import importlib.util
    file_suffix = '.py'
    LOGGER.debug('Loading transitions from \'{}\'.'.format(path))
    for file_path in glob.glob(os.path.join(path, '*{}'.format(file_suffix))):
        LOGGER.debug('Loading transitions from \'{}\'.'.format(file_path))
        module_name = os.path.basename(file_path)[:-len(file_suffix)]
        module_spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(module_spec)
        try:
            module_spec.loader.exec_module(module)
        except Exception as e:
            LOGGER.warning('Could not load module {}: {}'.format(
                module_name, str(e)))
            continue
        if not hasattr(module, 'TRANSITIONS'):
            LOGGER.debug('Module {} has no TRANSITIONS.'.format(module_name))
            continue
        loaded_count = 0
        for new_transition_data in module.TRANSITIONS:
            try:
                new_transition = TRANSITION_SCHEMA(new_transition_data)
                new_transition = Transition(
                    SchemaVersion(*new_transition['from']),
                    SchemaVersion(*new_transition['to']),
                    new_transition_data['function'])
            except voluptuous.MultipleInvalid as e:
                LOGGER.warning(
                    'Invalid transition data in module {}: {}'.format(
                        module_name, str(e)))
                continue
            if any(new_transition.from_version == my_transition.from_version
                   and new_transition.to_version == my_transition.to_version
                   for my_transition in TRANSITIONS):
                LOGGER.warning(
                    '{} is provided more than once. Only the first is used.'.
                    format(new_transition))
            else:
                TRANSITIONS.append(new_transition)
                LOGGER.debug('Loaded {} from \'{}\'.'.format(
                    new_transition, module_name))
                loaded_count += 1
        LOGGER.info('Loaded {} transitions from \'{}\'.'.format(
            loaded_count, module_name))


def decide_migration_path(from_version: SchemaVersion,
                          to_version: SchemaVersion
                          ) -> typing.Optional[typing.List[Transition]]:
    def find_all_transitions_paths(from_version: SchemaVersion,
                                   to_version: SchemaVersion
                                   ) -> typing.List[typing.List[Transition]]:
        """Returns a list of all known transition paths from @a from_version to @a to_version."""
        def depth_search(current_path) -> typing.List[typing.List[Transition]]:
            matching_paths = list()
            makes_no_cycle = lambda transition: transition.from_version not in current_path
            connects = lambda transition: transition.to_version == current_path[-1]
            for transition in filter(makes_no_cycle, filter(connects, TRANSITIONS)):
                if transition.from_version == from_version:
                    # found a path
                    matching_paths.append([transition])
                else:
                    # search deeper
                    current_path.append(transition.from_version)
                    for path in depth_search(current_path):
                        path.append(transition)
                        matching_paths.append(path)
                    current_path.pop()
            return matching_paths

        matching_paths = depth_search([to_version])
        matching_paths.sort(key=len)
        return matching_paths

    transition_paths = find_all_transitions_paths(from_version, to_version)
    if len(transition_paths) == 0:
        LOGGER.error(
            'Cannot perform migration: failed to find a transition path from {} to {}. '
            'Please check the schema versions and if they are correct, consider '
            'contributing the missing transition.'.format(
                from_version.toString(), to_version.toString()))
        return None

    def format_path(path):
        return ' -> '.join(
            (from_version.toShortString(),
             *(transition.to_version.toShortString() for transition in path)))

    print('Found the following applicable transition paths '
          'compatible with iroha, compiled for DB version {}:'.format(
              to_version.toShortString()))
    while True:
        for idx, path in enumerate(transition_paths):
            print('{}:  {}'.format(idx, format_path(path)))
        print(
            'Enter the index of migration path to perform '
            'or strat typing \'cancel\' to abort: ',
            end='')
        answer = input()
        if 'cancel'.startswith(answer.lower()):
            LOGGER.debug('User cancelled the migration.')
            return None
        if answer.isdecimal():
            choice = int(answer)
            if choice < len(transition_paths):
                chosen_path = transition_paths[choice]
                LOGGER.debug('User chose migration path {}.'.format(
                    format_path(chosen_path)))
                return chosen_path
        print('Input not interpreted. Try again.')


def migrate_to(connection, to_version: SchemaVersion) -> None:
    current_version = get_current_db_version(connection)
    if current_version is None:
        LOGGER.error(
            'Cannot perform migration: failed to get current DB schema version. '
            'Please force set the schema version to the version of iroha '
            'that created this schema. Consider reading the documentation first: {}'.format(MIGRATION_DOCS_URL))
        return

    chosen_path = decide_migration_path(current_version, to_version)
    if chosen_path is None:
        return

    try:
        cursor = connection.cursor()
        for transition in chosen_path:
            LOGGER.info('Migrating from {} to {}.'.format(transition.from_version, transition.to_version))
            transition.function(cursor)
        connection.commit()
    except:
        LOGGER.info('Migration failed, rolling back.')
        connection.rollback()
        raise
    force_schema_version(connection, to_version)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for _, param in PARAMS.items():
        parser.add_argument(
            '--{}'.format(param.cmd_line_arg),
            help='{} Can also be set with {} environment variable.'.format(
                param.descr, param.env_key),
            default=param.default,
            required=False)

    parser.add_argument('-v',
                        '--verbosity',
                        choices=logging._nameToLevel,
                        default='INFO',
                        help='logging verbosity')

    parser.add_argument(
        '--force_schema_version',
        action='store_true',
        help='Perform no migration, just set the schema version.')

    parser.add_argument(
        '--transitions_dir',
        action='append',
        default=[DEFAULT_TRANSITIONS_DIR],
        help='Perform no migration, just set the schema version.')

    args = parser.parse_args()
    logging.basicConfig(level=args.verbosity)
    get_params(args)

    target_schema_version = SchemaVersion(PARAMS['iroha_version_major'],
                                          PARAMS['iroha_version_minor'],
                                          PARAMS['iroha_version_patch'])

    connection = psycopg2.connect(
        "host={pg_ip} port={pg_port} dbname={pg_dbname} user={pg_user} password={pg_password}"
        .format(**PARAMS))

    if args.force_schema_version:
        force_schema_version(connection, target_schema_version)
    else:
        for transitions_dir in args.transitions_dir:
            load_transitions_from_dir(transitions_dir)
        migrate_to(connection, target_schema_version)
