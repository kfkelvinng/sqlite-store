import pickle
import bz2
import gzip

import sqlite3


class Connection:
    def __init__(self, path_str=":memory:", mode="rw"):
        if "w" not in mode:
            import pathlib
            uri_str = pathlib.Path(path_str).absolute().as_uri() + "?mode=ro"
            self._conn = sqlite3.Connection(uri_str, check_same_thread=False, uri=True)
        else:
            self._conn = sqlite3.Connection(path_str, check_same_thread=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if self._conn is not None:
            self.flush()
            self._conn.close()
            self._conn = None

    def flush(self):
        self._conn.commit()

    def shelve(self, shelve_name: str = None, mode="rw") -> "SqliteShelve":
        if "w" not in mode:
            assert shelve_name in map(lambda t: t[1], self.sqlite_schema())
        return SqliteShelve(self, table_name=shelve_name)

    def sqlite_schema(self):
        return self._conn.execute("SELECT * from sqlite_schema").fetchall()

    def table_list(self):
        """PRAGMA table_list is not always available. Use sqlite_schema instead for cross-platform"""
        return self._conn.execute("PRAGMA table_list").fetchall()

    def table_info(self, table_name):
        return self._conn(f"PRAGMA table_info([{table_name}]);").fetchall()

from typing import TypeVar, Generic

V = TypeVar('V')

class SqliteShelve(Generic[V]):
    def __init__(self, _conn_sh: "Connection", table_name: str = None):
        self.table_name = "SHELVE" if table_name is None else table_name
        self._conn = _conn_sh._conn
        self._conn_sh = _conn_sh
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (NAME TEXT PRIMARY KEY ON CONFLICT REPLACE, DATA BLOB)")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn_sh.flush()

    def __getitem__(self, item: str) -> V:
        name, blb = self._conn.execute(f"SELECT NAME, DATA FROM {self.table_name} WHERE NAME=?", [item]).fetchone()
        return self._decompress(blb)

    @staticmethod
    def _decompress(blb):
        if blb[0:2] == b'\x1f\x8b':
            return pickle.loads(gzip.decompress(blb))
        return pickle.loads(bz2.decompress(blb))

    def __setitem__(self, key: str, value: V):
        self._conn.execute(f"INSERT INTO {self.table_name} (NAME, DATA) VALUES (?,?)",
                           [key, gzip.compress(pickle.dumps(value))])

    def __delitem__(self, key: str):
        self._conn.execute(f"DELETE from {self.table_name} where NAME=(?)", [key])

    def __contains__(self, item: str):
        c, = self._conn.execute(f"SELECT COUNT(*) FROM {self.table_name} WHERE NAME=?", [item]).fetchone()
        return c == 1

    def __len__(self):
        c, = self._conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()
        return c

    def acquire(self, key: str, value: V):
        return self._conn.execute(f"INSERT INTO {self.table_name} (NAME, DATA) VALUES (?,?) ON CONFLICT DO NOTHING",
                           [key, gzip.compress(pickle.dumps(value))]).rowcount == 1

    def keys(self):
        return map(lambda x: x[0], self._conn.execute(f"SELECT NAME FROM {self.table_name}"))

    def values(self):
        return map(lambda x: self._decompress(x[1]), self._conn.execute(f"SELECT NAME, DATA FROM {self.table_name}"))

    def items(self):
        return map(lambda x: (x[0], self._decompress(x[1])),
                   self._conn.execute(f"SELECT NAME, DATA FROM {self.table_name}"))
