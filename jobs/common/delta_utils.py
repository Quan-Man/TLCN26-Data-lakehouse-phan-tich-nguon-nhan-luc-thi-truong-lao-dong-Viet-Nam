"""Compatibility wrappers for legacy path-based Delta reads.

Mutating Delta operations must use ``common.delta_io`` so a table's catalog
write policy, key, and partition scope are enforced.
"""


def read_delta(spark, path: str, *, version=None):
    reader = spark.read.format("delta")
    if version is not None:
        if type(version) is not int or version < 0:
            raise ValueError("version must be an integer >= 0")
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def write_delta(*args, **kwargs):
    raise RuntimeError("Path-based Delta writes are disabled; use common.delta_io.write_delta(table_name, df)")


def merge_delta(*args, **kwargs):
    raise RuntimeError("Path-based Delta merges are disabled; use common.delta_io.write_delta(table_name, df)")
