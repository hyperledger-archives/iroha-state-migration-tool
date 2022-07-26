import os
import sys
import typing
import importlib.util
import google.protobuf.json_format
import binascii
import hashlib
import multiprocessing

from schema_version import *

SCHEMA_DIR = 'schema'


class BlockStorageBase:
    @staticmethod
    def get_block_hash(block) -> bytes:
        '''@return hex hash of the block.
        pray that it matches the one calculated in iroha!
        (there are no guarantees of identical serialization of protobuf)
        '''
        hasher = hashlib.sha3_256()
        hasher.update(block.block_v1.payload.SerializeToString())
        return binascii.hexlify(hasher.digest())


class BlockStorageFiles(BlockStorageBase):
    def __init__(self, path, schema):
        self._path = path
        self._schema = schema

    def iterate(self):
        height = 1
        while True:
            block = self.load_at_height(height)
            if block is None:
                break
            yield block
            height += 1

    def iterate_with_callable(self, func):
        'WARNING func must be thread safe!'
        current_height = multiprocessing.Value('i')
        current_height.value = 0

        def load_and_feed_next_block():
            def load_block(height=current_height):
                with height.get_lock():
                    height_local = height.value
                    height.value += 1
                    return self.load_at_height(height_local)

            for block in iter(load_block, None):
                func(block)

        pool = list()

        for _ in range(multiprocessing.cpu_count()):
            process = multiprocessing.Process(target=load_and_feed_next_block)
            process.start()
            pool.append(process)

        for process in pool:
            process.join()

        for process in pool:
            if process.exitcode != 0:
                raise Exception(
                    f'BlockStorageFiles: child process {process.name} exited with code {process.exitcode}'
                )

    def _get_block_file_path_at_height(self, height: int) -> str:
        return os.path.join(self._path, '{0:0>16}'.format(height))

    def load_at_height(self, height: int):
        block_file_path = self._get_block_file_path_at_height(height)
        if not os.path.isfile(block_file_path):
            return None
        with open(block_file_path, 'rt') as block_file:
            return google.protobuf.json_format.Parse(block_file.read(),
                                                     self._schema.Block())

    def get_top_block_height(self) -> typing.Optional[int]:

        # listing a directory with very many files is longer than querying single file names
        block_exists = lambda height: os.path.isfile(
            self._get_block_file_path_at_height(height))

        if not block_exists(1): return None

        scope = [1, 1]

        # incremental search
        while block_exists(scope[1]):
            scope = [scope[1], scope[1] * 2]

        # binary search
        while scope[0] + 1 < scope[1]:
            p = scope[0] + (scope[1] - scope[0]) // 2
            if block_exists(p):
                scope[0] = p
            else:
                scope[1] = p

        assert block_exists(scope[0])
        assert not block_exists(scope[1])

        return scope[0]


class BlockStorageSql(BlockStorageBase):
    def __init__(self, cursor, schema):
        self._cursor = cursor
        self._schema = schema

    def _block_from_hex(self, block_hex):
        block = self._schema.Block()
        block.ParseFromString(binascii.unhexlify(block_hex))
        return block

    def iterate(self):
        self._cursor.execute(
            'select block_data from blocks order by height asc')
        while True:
            row = self._cursor.fetchone()
            if row is None:
                break
            yield self._block_from_hex(row[0])

    def load_at_height(self, height: int):
        self._cursor.execute('select block_data from blocks where height = %s',
                             height)
        rows = self._cursor.fetchall()
        if len(row) == 0:
            return None
        return self._block_from_hex(rows[0][0])

    def get_top_block_height(self) -> typing.Optional[int]:
        self._cursor.execute('select max(height) from blocks')
        rows = self._cursor.fetchall()
        if len(row) == 0:
            return None
        return self._block_from_hex(rows[0][0])


def load_block_schema(version: SchemaVersion):
    schema_package_path = os.path.join(
        SCHEMA_DIR, '{}_{}_{}'.format(version.iroha_major, version.iroha_minor,
                                      version.iroha_patch))
    if not os.path.isdir(schema_package_path):
        raise Exception(
            'Schema files not found in {}.'.format(schema_package_path))
    sys.path.append(schema_package_path)
    block_module_path = os.path.join(schema_package_path, 'block_pb2.py')
    block_module_spec = importlib.util.spec_from_file_location(
        'block_pb2', block_module_path)
    block_module = importlib.util.module_from_spec(block_module_spec)
    try:
        block_module_spec.loader.exec_module(block_module)
    except Exception as e:
        raise Exception('Could not load schema module {}: {}'.format(
            block_module_path, str(e))) from e
    return block_module


def get_block_storage(block_storage_files_path: typing.Optional[str], cursor,
                      version: SchemaVersion):
    block_schema = load_block_schema(version)
    if block_storage_files_path is not None:
        return BlockStorageFiles(block_storage_files_path, block_schema)
    return BlockStorageSql(cursor, block_schema)
