import sqlite3
import sqlite_store.shelve


class Connection:
    def __init__(self, path_str=":memory:"):
        self._conn = sqlite3.Connection(path_str, check_same_thread=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.flush()
        self._conn.close()

    def flush(self):
        self._conn.commit()

    def shelve(self, shelve_name: str = None) -> sqlite_store.shelve.SqliteShelve:
        return sqlite_store.shelve.SqliteShelve(self._conn, table_name=shelve_name)

    def table_list(self):
        return self._conn.execute("PRAGMA table_list").fetchall()

    def table_info(self, table_name):
        return self._conn(f"PRAGMA table_info([{table_name}]);").fetchall()
