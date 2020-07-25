import itertools
import json
import multiprocessing


def migrate_1_1_3_to_1_2_0(cursor, block_storage):
    # added on chain TLS certs to peers
    cursor.execute('alter table peer add tls_certificate varchar')

    # assets have no description
    cursor.execute('alter table asset drop data')

    # added 6 role permissions: kCallEngine, kGetAllEngineReceipts, kGetDomainEngineReceipts, kGetMyEngineReceipts, kGrantCallEngineOnMyBehalf, kRoot
    cursor.execute(
        "alter table role_has_permissions alter permission type bit(53) using '0'::bit(6) || permission"
    )

    # added 1 grantable permission: kCallEngineOnMyBehalf
    cursor.execute(
        "alter table account_has_grantable_permissions alter permission type bit(6) using '0'::bit(1) || permission"
    )

    # added on-chain settings
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS setting(
            setting_key text,
            setting_value text,
            PRIMARY KEY (setting_key)
        );
        ''')

    # tables position_by_hash, height_by_account_set, index_by_creator_height and position_by_account_asset are merged into tx_positions

    # first, load all tx timestamps from block storage to DB
    cursor.execute(
        'create table blockstorage_data (h bigint, i bigint, t bigint)')

    def get_block_data(block, output_queue):
        height = block.block_v1.payload.height
        for index, tx in enumerate(block.block_v1.payload.transactions):
            output_queue.put(
                (height, index, tx.payload.reduced_payload.created_time))

    def submit_block_data(block_data_to_submit):
        BULK_SZ = 1000
        data = iter(block_data_to_submit.get, 'stop')
        while True:
            chunk = tuple(itertools.islice(data, BULK_SZ))
            if len(chunk) == 0: break
            cursor.execute(
                'insert into blockstorage_data (h, i, t) values {}'.format(
                    ', '.join(map(str, chunk))))

    multiprocessing_manager = multiprocessing.Manager()
    block_data_to_submit = multiprocessing_manager.Queue()

    block_data_submitter = multiprocessing.Process(
        target=submit_block_data, args=(block_data_to_submit, ))
    block_data_submitter.start()

    # I could not parallelize block loading, but it would be nice
    for block in block_storage.iterate():
        get_block_data(block, block_data_to_submit)

    block_data_to_submit.put('stop')

    block_data_submitter.join()

    # now merge the tables and block storage data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tx_positions (
            creator_id text,
            hash varchar(64) not null,
            asset_id text,
            ts bigint,
            height bigint,
            index bigint
        );

        -- aux constraint to avoid duplicate rows
        alter table tx_positions
            add constraint aux_tx_positions_unique_constraint unique (creator_id, asset_id, height, index);

        -- index tx creators
        insert into tx_positions (creator_id, hash, asset_id, ts, height, index)
        (
            select
                    index_by_creator_height.creator_id
                  , position_by_hash.hash
                  , null
                  , blockstorage_data.t
                  , position_by_hash.height
                  , position_by_hash.index
            from position_by_hash
            inner join index_by_creator_height on
                    position_by_hash.height = index_by_creator_height.height
                and position_by_hash.index = index_by_creator_height.index
            left join blockstorage_data on
                    position_by_hash.height = blockstorage_data.h
                and position_by_hash.index = blockstorage_data.i
        );

        -- index asset transactions
        insert into tx_positions (creator_id, hash, asset_id, ts, height, index)
        (
            select 
                    position_by_account_asset.account_id
                  , position_by_hash.hash
                  , position_by_account_asset.asset_id
                  , blockstorage_data.t
                  , position_by_hash.height
                  , position_by_hash.index
            from position_by_hash
            inner join position_by_account_asset on
                    position_by_hash.height = position_by_account_asset.height
                and position_by_hash.index = position_by_account_asset.index
            left join blockstorage_data on
                    position_by_hash.height = blockstorage_data.h
                and position_by_hash.index = blockstorage_data.i
        )
        on conflict (creator_id, asset_id, height, index) do nothing;

        -- drop the old tables
        drop table position_by_hash cascade;
        drop table height_by_account_set cascade;
        drop table index_by_creator_height cascade;
        drop table position_by_account_asset cascade;

        -- drop aux stuff
        drop table blockstorage_data;
        alter table tx_positions drop constraint aux_tx_positions_unique_constraint cascade;

        -- create indices
        CREATE INDEX IF NOT EXISTS tx_positions_hash_index
            ON tx_positions
            USING hash
            (hash);
        CREATE INDEX IF NOT EXISTS tx_positions_creator_id_asset_index
            ON tx_positions
            (creator_id, asset_id);
        CREATE INDEX IF NOT EXISTS tx_positions_ts_height_index_index
            ON tx_positions
            (ts);
            ''')

    # burrow stuff
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS engine_calls (
            call_id serial unique not null,
            tx_hash text,
            cmd_index bigint,
            engine_response text,
            callee varchar(40),
            created_address varchar(40),
            PRIMARY KEY (tx_hash, cmd_index)
        );
        CREATE TABLE IF NOT EXISTS burrow_account_data (
            address varchar(40),
            data text,
            PRIMARY KEY (address)
        );
        CREATE TABLE IF NOT EXISTS burrow_account_key_value (
            address varchar(40),
            key varchar(64),
            value text,
            PRIMARY KEY (address, key)
        );
        CREATE TABLE IF NOT EXISTS burrow_tx_logs (
            log_idx serial primary key,
            call_id integer references engine_calls(call_id),
            address varchar(40),
            data text
        );
        CREATE TABLE IF NOT EXISTS burrow_tx_logs_topics (
            topic varchar(64),
            log_idx integer references burrow_tx_logs(log_idx)
        );
        CREATE INDEX IF NOT EXISTS burrow_tx_logs_topics_log_idx
            ON burrow_tx_logs_topics
            USING btree
            (log_idx ASC);''')


def migrate_1_2_0_to_1_1_3(cursor, block_storage):
    # revert added on chain TLS certs to peers
    cursor.execute('alter table peer drop tls_certificate')

    # revert assets have no description. we still cannot access them, so empty values are ok
    cursor.execute('alter table asset add data varchar')

    # revert 6 role permissions: kCallEngine, kGetAllEngineReceipts, kGetDomainEngineReceipts, kGetMyEngineReceipts, kGrantCallEngineOnMyBehalf, kRoot
    cursor.execute(
        "alter table role_has_permissions alter permission type bit(47) using permission::bigint::bit(47)"
    )

    # revert added 1 grantable permission: kCallEngineOnMyBehalf
    cursor.execute(
        "alter table account_has_grantable_permissions alter permission type bit(5) using permission::integer::bit(5)"
    )

    # revert tables position_by_hash, height_by_account_set, index_by_creator_height and position_by_account_asset are merged into tx_positions
    cursor.execute('''
        CREATE TABLE position_by_hash (
            hash varchar,
            height bigint,
            index bigint
        );
        CREATE TABLE height_by_account_set (
            account_id text,
            height bigint
        );
        CREATE TABLE index_by_creator_height (
            id serial,
            creator_id text,
            height bigint,
            index bigint
        );
        CREATE TABLE position_by_account_asset (
            account_id text,
            asset_id text,
            height bigint,
            index bigint
        );

        -- populate position_by_hash
        insert into position_by_hash (hash, height, index)
        (
            select distinct
                    tx_positions.hash
                  , tx_positions.height
                  , tx_positions.index
            from tx_positions
        );

        -- populate height_by_account_set
        insert into height_by_account_set (account_id, height)
        (
            select distinct
                    tx_positions.creator_id
                  , tx_positions.height
            from tx_positions
        );

        -- populate index_by_creator_height
        insert into index_by_creator_height (creator_id, height, index)
        (
            select distinct
                    tx_positions.creator_id
                  , tx_positions.height
                  , tx_positions.index
            from tx_positions
            where asset_id is null
        );

        -- populate position_by_account_asset
        insert into position_by_account_asset (account_id, asset_id, height, index)
        (
            select distinct
                    tx_positions.creator_id
                  , tx_positions.asset_id
                  , tx_positions.height
                  , tx_positions.index
            from tx_positions
            where asset_id is not null
        );

        -- drop old tables
        drop table tx_positions cascade;
            ''')

    # revert burrow stuff
    cursor.execute('''
        drop table engine_calls cascade;
        drop table burrow_account_data cascade;
        drop table burrow_account_key_value cascade;
        drop table burrow_tx_logs cascade;
        drop table burrow_tx_logs_topics cascade;
            ''')

    # revert added on-chain settings
    cursor.execute('drop table setting')


TRANSITIONS = (
    {
        'from': ('1.1.3'),
        'to': ('1.2.0'),
        'function': migrate_1_1_3_to_1_2_0
    },
    {
        'from': ('1.2.0'),
        'to': ('1.1.3'),
        'function': migrate_1_2_0_to_1_1_3
    },
)
