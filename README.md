# iroha-state-migration-tool
A tool to perform migration of Iroha state database versions for different versions of Iroha.

## Usage

synopsis:
```
state_migration.py <options>
```

### common options
```
  -h, --help            show help message and exit
  -v, --verbosity {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG}
                        logging verbosity
  -p, --print_current_version
                        Fetch and print current schema version before any
                        other actions.
```

### database connection options
```
  --pg_ip PG_IP         PostgreSQL WSV database IP address. Can also be set
                        with IROHA_POSTGRES_HOST environment variable.
  --pg_port PG_PORT     PostgreSQL WSV database port. Can also be set with
                        IROHA_POSTGRES_PORT environment variable.
  --pg_user PG_USER     PostgreSQL WSV database username. Can also be set with
                        IROHA_POSTGRES_USER environment variable.
  --pg_password PG_PASSWORD
                        PostgreSQL WSV database password. Can also be set with
                        IROHA_POSTGRES_PASSWORD environment variable.
  --pg_dbname PG_DBNAME
                        PostgreSQL WSV database name. Can also be set with
                        IROHA_POSTGRES_DBNAME environment variable.
```

### migration options
```
  --target_schema_version TARGET_SCHEMA_VERSION
                        Target database schema version Can also be set with
                        IROHA_TARGET_SCHEMA_VERSION environment variable.
  --force_schema_version
                        Perform no migration, just set the schema version.
  --transitions_dir TRANSITIONS_DIR
                        Perform no migration, just set the schema version.
```

### block storage options
If you use database block storage, do not specify `--block_storage_files`.
```
  --block_storage_files BLOCK_STORAGE_FILES
                        Path to block storage, if filesystem is used. Can also
                        be set with IROHA_BLOCK_STORAGE_PATH environment
                        variable.
```


## Adding migration scripts
To add a new migration script, just create a file for your case like this:

```python
def my_migration_1(cursor, block_storage):
  cursor.execute('create table my_nice_table ( xor_price really_big_integer );

TRANSITIONS = (
    {
        'from': ('1.5.9'),
        'to': ('1.21.3'),
        'function': my_migration_1
    },
)
```

You can implement as many migration functions as you want in a single file.
Just do not forget to add them to `TRANSITIONS` tuple so that the main `state_migration.py` script knows what do they do.

Then either put your file to common `migration_data` directory and open a PR or keep it at a separate collection.
Just keep in mind that `state_migration.py` searches for migration functions in `migration_data` and in additional directories provided with `--transitions_dir` argument (which may occur multiple times).
