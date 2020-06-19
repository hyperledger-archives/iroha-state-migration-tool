import itertools


def migrate_1_1_1_to_1_1_2(cursor, block_storage):
    # the table holding DB version must be already created, since the migration would not start otherwise

    # added top block info
    cursor.execute('''
        create table top_block_info (
            lock char(1) default 'X' not null primary key,
            height int,
            hash character varying(128)
        );
        ''')

    maybe_top_height = block_storage.get_top_block_height()
    if maybe_top_height is not None:
        top_block = block_storage.load_at_height(maybe_top_height)
        cursor.execute(
            '''
            insert into top_block_info (height, hash)
            values (%s, %s)
            on conflict (lock) do update
            set height = excluded.height, hash = excluded.hash''',
            (top_block.block_v1.payload.height,
             block_storage.get_block_hash(top_block).decode()))


def migrate_1_1_2_to_1_1_1(cursor, block_storage):
    # revert added top block info
    cursor.execute('drop table top_block_info')


TRANSITIONS = (
    {
        'from': ('1.1.1'),
        'to': ('1.1.2'),
        'function': migrate_1_1_1_to_1_1_2
    },
    {
        'from': ('1.1.2'),
        'to': ('1.1.1'),
        'function': migrate_1_1_2_to_1_1_1
    },
)
