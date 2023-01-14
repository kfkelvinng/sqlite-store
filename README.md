# A Shelve and Document Store backed by Sqlite3

Minimum extenral dependencies
Install simply drop into your project by downloading as zip or add a git submodule
```

```

https://www.sqlite.org/json1.html


import sqlite3
c=sqlite3.Connection(":memory:")
c.execute("CREATE TABLE J(D)")
r=c.execute("INSERT INTO J VALUES(JSON_OBJECT('someKey', 'someValue'));")
r=c.execute("SELECT * FROM J WHERE JSON_EXTRACT(D, '$.someKey')='someValue'")
r.fetchall()


c.execute("EXPLAIN query plan SELECT * FROM J WHERE JSON_EXTRACT(D, '$.someKey')='someValue'").fetchall()

r=c.execute("CREATE INDEX test_idx ON J(JSON_EXTRACT(D, '$.someKey'));")
c.execute("PRAGMA compile_options;")
c.execute("PRAGMA compile_options;").fetchall()
('ENABLE_FTS3',),
 ('ENABLE_FTS3_PARENTHESIS',),
 ('ENABLE_FTS3_TOKENIZER',),
 ('ENABLE_FTS4',),
 ('ENABLE_FTS5',),
 ('ENABLE_JSON1',),


are built into SQLite by default, as of SQLite version 3.38.0 (2022-02-22)


are built into SQLite by default, as of SQLite version 3.38.0 (2022-02-22)
>>> con.enable_load_extension(True)
>>> con.load_extension("./json1.so")