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


def parse_schema_version(version_string: str) -> SchemaVersion:
    try:
        iroha_major, iroha_minor, iroha_patch = map(int,
                                                    version_string.split('.'))
        return SchemaVersion(iroha_major, iroha_minor, iroha_patch)
    except Exception as e:
        raise ValueError('Could not parse Schema version.') from e
