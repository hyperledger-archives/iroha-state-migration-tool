import os
import sys
import typing
import importlib.util
import google.protobuf.json_format
import binascii

from schema_version import *

SCHEMA_DIR = 'schema'


class BlockStorageFiles:
    def __init__(self, path, schema):
        self._path = path
        self._schema = schema

    def iterate(self):
        height = 1
        while True:
            block = load_at_height(height)
            if block is None:
                break
            yield block
            height += 1

    def _get_block_file_path_at_height(self, height: int) -> str:
        return os.path.join(self._path, '{0:0>16}'.format(height))

    def load_at_height(self, height: int):
        block_file_path = self._get_block_file_path_at_height(height)
        if not os.path.isfile(block_file_path):
            return None
        with open(block_file_path, 'rt') as block_file:
            return json_format.Parse(block_file.read(), self._schema.Block())


class BlockStorageSql:
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
