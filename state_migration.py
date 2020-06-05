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


class SchemaVersion:
    def __init__(self, iroha_major, iroha_minor, iroha_patch):
        self.iroha_major = iroha_major
        self.iroha_minor = iroha_minor
        self.iroha_patch = iroha_patch

    def toString(self):
        return str(self.__dict__)

    def toShortString(self):
        return '.'.join(
            map(str, (self.iroha_major, self.iroha_minor, self.iroha_patch)))

    def __eq__(self, rhs):
        return self.__dict__ == rhs.__dict__

    def __repr__(self):
        return 'SchemaVersion: {}'.format(self.toShortString())


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


def check_directory(val: str) -> str:
    if not os.path.isdir(val):
        raise ValueError('No such directory.')
    return val


def parse_schema_version(version_string: str) -> SchemaVersion:
    try:
        iroha_major, iroha_minor, iroha_patch = map(int,
                                                    version_string.split('.'))
        return SchemaVersion(iroha_major, iroha_minor, iroha_patch)
    except Exception as e:
        raise ValueError('Could not parse Schema version.') from e


VERSION_SCHEMA = voluptuous.Schema(parse_schema_version)
TRANSITION_SCHEMA = voluptuous.Schema({
    'from': VERSION_SCHEMA,
    'to': VERSION_SCHEMA,
    'function': callable
})

DB_PARAMS = {
    'pg_ip':
    UserParam(
        'pg_ip',
        'IROHA_POSTGRES_HOST',
        'PostgreSQL WSV database IP address.',
        check_nonempty_string,
    ),
    'pg_port':
    UserParam(
        'pg_port',
        'IROHA_POSTGRES_PORT',
        'PostgreSQL WSV database port.',
        check_convert_nonnegative_int,
        5432,
    ),
    'pg_user':
    UserParam(
        'pg_user',
        'IROHA_POSTGRES_USER',
        'PostgreSQL WSV database username.',
        check_nonempty_string,
    ),
    'pg_password':
    UserParam('pg_password', 'IROHA_POSTGRES_PASSWORD',
              'PostgreSQL WSV database password.'),
    'pg_dbname':
    UserParam(
        'pg_dbname',
        'IROHA_POSTGRES_DBNAME',
        'PostgreSQL WSV database name.',
        check_nonempty_string,
    ),
}

MIGRATION_PARAMS = {
    'target_schema_version':
    UserParam(
        'target_schema_version',
        'IROHA_TARGET_SCHEMA_VERSION',
        'Target database schema version',
        parse_schema_version,
    ),
}

BLOCK_STORAGE_FILES_PARAMS = {
    'block_storage_files':
    UserParam(
        'block_storage_files',
        'IROHA_BLOCK_STORAGE_PATH',
        'Path to block storage, if filesystem is used.',
        check_directory,
    ),
}

ALL_PARAMS = (DB_PARAMS, MIGRATION_PARAMS, BLOCK_STORAGE_FILES_PARAMS)


def get_params(params, args: argparse.Namespace, required: bool) -> None:
    """Substitutes @a params values with user provided data."""
    def get_raw(param_name):
        param = params[param_name]
        if hasattr(args, param_name):
            val = getattr(args, param_name)
            if val is not None:
                return val
        if param.env_key in os.environ:
            return os.environ[param.env_key]
        if required:
            print(
                'You have not specified {} '
                'You can set it via command line key {} '
                'or environment variable {}. '
                'Alternatively, you can type the value here: '.format(
                    param.descr, param.cmd_line_arg, param.env_key),
                end='',
            )
            return input()
        return None

    def get_transformed(param_name):
        transformer = params[param_name].transformer
        raw_val = get_raw(param_name)
        return raw_val if transformer is None or raw_val is None else transformer(
            raw_val)

    for param_name in params:
        params[param_name] = get_transformed(param_name)
        LOGGER.debug('Using {} = {}'.format(param_name, params[param_name]))


def get_current_db_version(connection) -> typing.Optional[SchemaVersion]:
    cur = connection.cursor()
    try:
        cur.execute(
            'select iroha_major, iroha_minor, iroha_patch from schema_version;'
        )
        version_data = cur.fetchall()
        LOGGER.debug('Fetched version data from DB: {}'.format(version_data))
        assert len(version_data) == 1
        return SchemaVersion(*version_data[0])
    except Exception as e:
        LOGGER.warning(
            'Could not read database schema version information: {}'.format(e))
        return None


def force_schema_version(connection, schema_version: SchemaVersion) -> None:
    LOGGER.info('Setting schema version to {}'.format(
        schema_version.toShortString()))
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
        """,
        schema_version.__dict__,
    )
    connection.commit()


class Transition:
    def __init__(
        self,
        from_version: SchemaVersion,
        to_version: SchemaVersion,
        function: typing.Callable,
    ):
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
        module_spec = importlib.util.spec_from_file_location(
            module_name, file_path)
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
                    new_transition['from'],
                    new_transition['to'],
                    new_transition_data['function'],
                )
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


def decide_migration_path(
        from_version: SchemaVersion,
        to_version: SchemaVersion) -> typing.Optional[typing.List[Transition]]:
    def find_all_transitions_paths(
            from_version: SchemaVersion,
            to_version: SchemaVersion) -> typing.List[typing.List[Transition]]:
        """Returns a list of all known transition paths from @a from_version to @a to_version."""
        def depth_search(current_path) -> typing.List[typing.List[Transition]]:
            matching_paths = list()
            makes_no_cycle = (
                lambda transition: transition.from_version not in current_path)
            connects = lambda transition: transition.to_version == current_path[
                -1]
            for transition in filter(makes_no_cycle,
                                     filter(connects, TRANSITIONS)):
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
                from_version.toShortString(), to_version.toShortString()))
        return None

    def format_path(path):
        return ' -> '.join((
            from_version.toShortString(),
            *(transition.to_version.toShortString() for transition in path),
        ))

    print('Found the following applicable transition paths '
          'compatible with iroha, compiled for DB version {}:'.format(
              to_version.toShortString()))
    while True:
        for idx, path in enumerate(transition_paths):
            print('{}:  {}'.format(idx, format_path(path)))
        print(
            'Enter the index of migration path to perform '
            'or strat typing \'cancel\' to abort: ',
            end='',
        )
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


def migrate_to(connection, block_storage_files_path: typing.Optional[str],
               to_version: SchemaVersion) -> None:
    current_version = get_current_db_version(connection)
    if current_version is None:
        LOGGER.error(
            'Cannot perform migration: failed to get current DB schema version. '
            'Please force set the schema version to the version of iroha '
            'that created this schema. Consider reading the documentation first: {}'
            .format(MIGRATION_DOCS_URL))
        return

    chosen_path = decide_migration_path(current_version, to_version)
    if chosen_path is None:
        return

    try:
        cursor = connection.cursor()
        for transition in chosen_path:
            LOGGER.info('Migrating from {} to {}.'.format(
                transition.from_version, transition.to_version))
            transition.function(cursor)
        connection.commit()
    except:
        LOGGER.info('Migration failed, rolling back.')
        connection.rollback()
        raise
    force_schema_version(connection, to_version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for params in ALL_PARAMS:
        for _, param in params.items():
            parser.add_argument(
                '--{}'.format(param.cmd_line_arg),
                help='{} Can also be set with {} environment variable.'.format(
                    param.descr, param.env_key),
                default=param.default,
                required=False,
            )

    parser.add_argument(
        '-v',
        '--verbosity',
        choices=logging._nameToLevel,
        default='INFO',
        help='logging verbosity',
    )

    parser.add_argument(
        '--force_schema_version',
        action='store_true',
        help='Perform no migration, just set the schema version.',
    )

    parser.add_argument(
        '--transitions_dir',
        action='append',
        default=[DEFAULT_TRANSITIONS_DIR],
        help='Perform no migration, just set the schema version.',
    )

    parser.add_argument(
        '-p',
        '--print_current_version',
        action='store_true',
        help='Fetch and print current schema version before any other actions.',
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.verbosity)

    get_params(DB_PARAMS, args, True)
    connection = psycopg2.connect(
        "host={pg_ip} port={pg_port} dbname={pg_dbname} user={pg_user} password={pg_password}"
        .format(**DB_PARAMS))

    if hasattr(args, 'print_current_version'):
        print("Current schema version is {}".format(
            get_current_db_version(connection)))

    get_params(MIGRATION_PARAMS, args, False)
    if MIGRATION_PARAMS['target_schema_version'] is not None:
        target_schema_version = MIGRATION_PARAMS['target_schema_version']
        get_params(BLOCK_STORAGE_FILES_PARAMS, args, False)
        if args.force_schema_version:
            force_schema_version(connection, target_schema_version)
        else:
            for transitions_dir in args.transitions_dir:
                load_transitions_from_dir(transitions_dir)
            migrate_to(connection,
                       BLOCK_STORAGE_FILES_PARAMS['block_storage_files'],
                       target_schema_version)
