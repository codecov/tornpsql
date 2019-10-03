#!/usr/bin/env python
import re
import os
import logging
import psycopg2
import psycopg2.extras
from select import select
from psycopg2.extras import Json
from psycopg2 import pool
from psycopg2.extensions import adapt
from psycopg2.extras import HstoreAdapter

# http://initd.org/psycopg/docs/module.html#exceptions
from psycopg2 import Warning
from psycopg2 import Error
from psycopg2 import InterfaceError
from psycopg2 import DatabaseError
from psycopg2 import DataError
from psycopg2 import OperationalError
from psycopg2 import IntegrityError
from psycopg2 import InternalError
from psycopg2 import ProgrammingError
from psycopg2 import NotSupportedError


__version__ = VERSION = version = '2.1.4'


_RE_WS = re.compile(r'\n\s*')
_RE_PSQL_URL = re.compile(r'^postgres://(?P<user>[^:]*):?(?P<password>[^@]*)@(?P<host>[^:]+):?(?P<port>\d+)/?(?P<database>[^#]+)(?P<search_path>#.+)?(?P<timezone>@.+)?$')
MIN_CONNECTION_POOL = 1
MAX_CONNECTION_POOL = 5

class PubSub(object):
    def __init__(self, db):
        self._db = db
        self._cur = db.cursor()
        self._channels = []

    @property
    def channels(self):
        return list(self._channels)

    def subscribe(self, channels):
        assert type(channels) in (tuple, list), 'Invalid channels. Must be tuple or list of strings'
        self._channels = set(list(self._channels) + list(channels))

    def unsubscribe(self, channels=None):
        if channels:
            assert type(channels) in (tuple, list), 'Invalid channels. Must be tuple or list of strings'
            self._cur.execute(''.join(['UNLISTEN %s;' % c for c in list(channels)]))
            [self._channels.remove(channel) for channel in channels]
        else:
            self._cur.execute(''.join(['UNLISTEN %s;' % c for c in list(self._channels)]))
            self._channels = []

    def __iter__(self):
        while len(self._channels) > 0:
            if select([self._db], [], [], 5) != ([], [], []):
                self._db.poll()
                while self._db.notifies:
                    yield self._db.notifies.pop()

    def listen(self):
        assert self._channels, 'No channels to listen to.'
        for channel in self._channels:
            self._cur.execute('LISTEN %s;' % channel)
        return self


try:
    basestring
except NameError:
    basestring = str


class _Connection(object):
    def __init__(self, host_or_url=None, database=None, user=None, password=None, port=5432,
                 search_path=None, timezone=None, enable_logging=None):
        if enable_logging is not None:
            self._logging = enable_logging
        else:
            self._logging = (os.getenv('DEBUG') == 'TRUE' or os.getenv('LOGLVL') == 'DEBUG' or os.getenv('PG_LOG') == 'TRUE')

        if host_or_url is None:
            host_or_url = os.getenv('DATABASE_URL', '127.0.0.1')
        if host_or_url.startswith('postgres://'):
            try:
                args = _RE_PSQL_URL.match(host_or_url).groupdict()
            except:
                raise ValueError('PostgreSQL url is not a valid format postgres://user:password@host:post/database from %s' % host_or_url)
            else:
                self.host = args.get('host')
                self.database = args.get('database')
                self._search_path = search_path or args.pop('search_path', None)
                self._timezone = timezone or args.pop('timezone', None)
        else:
            self._search_path = search_path
            self._timezone = timezone
            self.host = host_or_url
            self.database = database
            args = dict(host=host_or_url, database=database, port=int(port),
                        user=user, password=password)

        self._db = None
        self._register_types = []
        self._change_path = None

        self.pool = pool.ThreadedConnectionPool(MIN_CONNECTION_POOL, MAX_CONNECTION_POOL, **args)
        try:
            self.reconnect()
        except Exception as err:  # pragma: no cover
            logging.error('%s: Cannot connect to PostgreSQL on postgresql://%s:<password>@%s:%s/%s',
                          str(type(err)), args['user'], self.host, args['port'], self.database)

        try:
            psycopg2.extras.register_hstore(self._db, globally=True)
        except ProgrammingError:
            pass
        psycopg2.extensions.register_adapter(dict, Json)

    def hstore(self, _dict):
        return HstoreAdapter(_dict)

    def json(self, element):
        return Json(element)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, '_db', None) is not None:
            self._db = None

    def _reconnect(self):
        """ Gets a new connection from the pool"""
        self._db = self.pool.getconn()

        if self._search_path:
            self.execute('set search_path=%s;' % self._search_path)

        if self._timezone:
            self.execute("set timezone='%s';" % self._timezone)

    def _reregister_types(self):
        """Registers existing types for a new connection"""
        for _type in self._register_types:
            psycopg2.extensions.register_type(psycopg2.extensions.new_type(*_type))

    def path(self, search_path):
        self._change_path = search_path
        return self

    def adapt(self, value):
        """Adapt any value into SQL safe string"""
        return adapt(value)

    def register_type(self, oids, name, casting):
        """Callback to register data types when reconnect
        """
        assert type(oids) is tuple
        assert isinstance(name, basestring)
        assert hasattr(casting, '__call__')
        self._register_types.append((oids, name, casting))
        psycopg2.extensions.register_type(psycopg2.extensions.new_type(oids, name, casting))

    def mogrify(self, query, *parameters, **kwargs):
        """From http://initd.org/psycopg/docs/cursor.html?highlight=mogrify#cursor.mogrify
        Return a query string after arguments binding.
        The string returned is exactly the one that would be sent to the database running
        the execute() method or similar.
        """
        cursor = self._cursor()
        try:
            if kwargs:
                res = cursor.mogrify(query % dict([(r[0], adapt(r[1])) for r in list(kwargs.items())]))
            else:
                res = cursor.mogrify(query, parameters)

            cursor.close()
            return res

        except:  # pragma: no cover
            cursor.close()
            raise

    def query(self, query, *parameters, **kwargs):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters or None, kwargs)
            if cursor.description:
                column_names = [column.name for column in cursor.description]
                res = [Row(zip(column_names, row)) for row in cursor.fetchall()]
                cursor.close()
                return res
        except:
            cursor.close()
            raise

    def iter(self, query, *parameters, **kwargs):
        """Returns a generator for records from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters or None, kwargs)
            if cursor.description:
                column_names = [column.name for column in cursor.description]
                while True:
                    record = cursor.fetchone()
                    if not record:
                        break
                    yield Row(zip(column_names, record))
            raise StopIteration

        except:
            cursor.close()
            raise

    def execute(self, query, *parameters, **kwargs):
        """Same as query, but do not process results. Always returns `None`."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwargs)

        except:
            raise

        finally:
            cursor.close()

    def get(self, query, *parameters, **kwargs):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters, **kwargs)
        if not rows:
            return None
        elif len(rows) > 1:
            raise ValueError('Multiple rows returned for get() query')
        else:
            return rows[0]

    def executemany(self, query, *parameters):
        """Executes the given query against all the given param sequences.
        """
        cursor = self._cursor()
        try:
            self._executemany(cursor, query, parameters)
            if cursor.description:
                column_names = [column.name for column in cursor.description]
                res = [Row(zip(column_names, row)) for row in cursor.fetchall()]
                cursor.close()
                return res

        except Exception:  # pragma: no cover
            cursor.close()
            raise

    def _ensure_connected(self):
        if self._db is None:
            self.reconnect()

    def _cursor(self):
        self._ensure_connected()
        try:
            return self._db.cursor()

        except:
            self.reconnect()
            return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwargs):
        try:
            if kwargs:
                query = query % dict([(r[0], adapt(r[1])) for r in list(kwargs.items())])
                self._log(query)
                cursor.execute(query)
            else:
                self._log(query, parameters)
                cursor.execute(query, parameters)

        except OperationalError as e:  # pragma: no cover
            logging.error("Error connecting to PostgreSQL on %s, %s", self.host, e)
            self.close()
            raise

    def _log(self, query, params=None):
        if self._logging:
            if params:
                query = self.mogrify(query, *params)
            logging.info(_RE_WS.sub(' ', query))

    def _executemany(self, cursor, query, parameters):
        """The function is mostly useful for commands that update the database:
           any result set returned by the query is discarded."""
        try:
            self._log(query)
            cursor.executemany(query, parameters)
        except OperationalError as e:  # pragma: no cover
            logging.error('Error connecting to PostgreSQL on %s, e', self.host, e)
            self.close()
            raise

    def pubsub(self):
        return PubSub(self._db)

    def file(self, path, _execute=True):
        base = os.path.dirname(path)
        with open(path) as r:
            sql = re.sub(r'\\ir\s(.*)', lambda m: self.file(os.path.join(base, m.groups()[0]), False), r.read())
        if _execute:
            cursor = self._cursor()
            map(cursor.execute, sql.split("\n-- EOF\n"))
            cursor.close()
        else:
            return sql

    @property
    def notices(self):
        """pops and returns all notices
        http://initd.org/psycopg/docs/connection.html#connection.notices
        """
        return [self._db.notices.pop()[8:].strip() for x in range(len(self._db.notices))]


class Connection(_Connection):
    def reconnect(self):
        self._reconnect()
        self._db.autocommit = True
        self._reregister_types()


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:  # pragma: no cover
            raise AttributeError(name)
