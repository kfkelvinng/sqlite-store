import pickle
import bz2
import gzip

import sqlite_store.connection


class SqliteShelve:
    def __init__(self, _conn: "sqlite_store.connection.Connection", table_name: str = None):
        self.table_name = "SHELVE" if table_name is None else table_name
        self._conn = _conn
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (NAME TEXT PRIMARY KEY ON CONFLICT REPLACE, DATA BLOB)")

    def __getitem__(self, item):
        name, blb = self._conn.execute(f"SELECT NAME, DATA FROM {self.table_name} WHERE NAME=?", [item]).fetchone()
        return self._decompress(blb)

    @staticmethod
    def _decompress(blb):
        if blb[0:2] == b'\x1f\x8b':
            return pickle.loads(gzip.decompress(blb))
        return pickle.loads(bz2.decompress(blb))

    def __setitem__(self, key, value):
        self._conn.execute(f"INSERT INTO {self.table_name} (NAME, DATA) VALUES (?,?)", [key, gzip.compress(pickle.dumps(value))])

    def __contains__(self, item):
        c, = self._conn.execute(f"SELECT COUNT(*) FROM {self.table_name} WHERE NAME=?", [item]).fetchone()
        return c == 1

    def __len__(self):
        c, = self._conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()
        return c

    def keys(self):
        return map(lambda x: x[0], self._conn.execute(f"SELECT NAME FROM {self.table_name}"))

    def values(self):
        return map(lambda x: self._decompress(x[1]), self._conn.execute(f"SELECT NAME, DATA FROM {self.table_name}"))

    def items(self):
        return map(lambda x: (x[0], self._decompress(x[1])), self._conn.execute(f"SELECT NAME, DATA FROM {self.table_name}"))
