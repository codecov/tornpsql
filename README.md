tornpsql
========

[![Build Status](https://secure.travis-ci.org/codecov/tornpsql.png)](http://travis-ci.org/codecov/tornpsql)  [![codecov.io](https://codecov.io/gh/codecov/tornpsql/branch/master/graph/badge.svg)](https://codecov.io/gh/codecov/tornpsql)

`pip install tornpsql`

## Featuring
- Query via `args` or `kwargs`
- Support [Search Path](#set-search_path)
- [Query from Files](#query-files)
- [Pubsub](#pubsub)
- Retrieve notices (`raise notice 'something';`) via `list(db.notices)`

## Usage
```python
# Method 1
import tornpsql
db = tornpsql.Connection("postgres://postgres-user:postgres-password@127.0.0.1:5432/postgres-db")
```

```python
import tornpsql
db = tornpsql.Connection("127.0.0.1", "database", "postgres", "postgres-user", 5432)
```

```py
# get one
db.get("SELECT col from table where col = %s limit 1", value)
# >>> {"col": "value"}

# get many
db.query("SELECT col from table where col = %s limit 2", value)
# >>> [{"col": "value"}, {"col": "value"}]
```

## Set search_path
Set the `search_path` for the duration of the proceeding query.

```python
# set the "default" search_path to public
db = tornpsql.Connection(search_path="public")
results = db.path("another_schema").query("select column from mytable")
results = db.path("another_schema,and_another").query("select column from mytable")
# this will use search path: "public"
results = db.query("select column from mytable")
```

## Query Files

```sql
-- main.sql
create table example (id serial primary key);
\ir other.sql
```

```sql
-- other.sql
insert into example values (1, 2, 3);
```

```python
db = tornpsql.Connection()
db.file("main.sql")
```

## PubSub
```python
db = tornpsql.Connection()
pubsub = db.pubsub()
pubsub.subscribe( ("channel_1", ) )
for notify in pubsub.listen():
    print notify.pid, notify.channel, notify.payload
```

#### Via Threading
```python
class PubSubThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.db = tornpsql.Connection()
        self.pubsub = self.db.pubsub()
        self.pubsub.subscribe( ("channel_1", "channel_2") )

    def run(self):
        for notify in self.pubsub.listen():
            print notify.pid, notify.channel, notify.payload


pubsub = PubSubThread()
```
