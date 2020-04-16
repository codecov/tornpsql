import os
import unittest
import tornpsql
from decimal import Decimal

from psycopg2._json import Json
from psycopg2.extras import HstoreAdapter


class ConnectionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        try:
            self.db = tornpsql.Connection(database="tornpsql")
        except:
            pass

    def test_file_include(self):
        "can reference files relatively"
        self.db.file(os.path.join(os.path.dirname(__file__), "example.sql"))
        self.assertTrue(self.db.get("SELECT true as t from public.users where name='Mr. Johnson' limit 1;").t)

    def test_connection_args(self):
        "test connect with args"
        db = tornpsql.Connection("127.0.0.1", "tornpsql", os.getenv("postgres", None))
        self.assertTrue(db.get("select true as connected").connected)

    def test_connection_via_url(self):
        "can test connect with args"
        db = tornpsql.Connection(os.getenv("ALTERNATE_DATABASE_URL"))
        self.assertTrue(db.get("select true as connected").connected)

    def test_invalid_connection_args(self):
        "can parse connection url"
        self.assertRaises(ValueError, tornpsql.Connection, "postgres://user:pass@server:port/database")
        self.assertRaises(ValueError, tornpsql.Connection, "postgres://server:port/")
        self.assertRaises(ValueError, tornpsql.Connection, "postgres://user:password@server:invalid/database")
        self.assertRaises(ValueError, tornpsql.Connection, "postgres://user:password@server/database")
        self.assertRaises(ValueError, tornpsql.Connection, "postgres://user:password@:5432")
        with self.assertRaises(ValueError) as ex:
          tornpsql.Connection("postgres://user:password@:5432")
        self.assertEqual(ex.exception.args[0], "PostgreSQL url is not a valid format postgres://user:password@host:port/database from postgres://XXXXXXXX:XXXXXXXX@:5432")

    def test_registering_type(self):
        "can register custom types"
        self.db.register_type((790, ), "MONEY",
                              lambda s, cur: Decimal(s.replace(',', '').replace('$', '')) if s is not None else None)
        # PS: never, ever, ever use money, use numeric type.
        self.assertEqual(self.db.get("select '5.99'::money as a;").a, Decimal('5.99'))
        # re-connect to see if the registration sticks
        self.db.close()
        self.assertEqual(self.db.get("select '5.99'::money as a;").a, Decimal('5.99'))

    def test_assert_one_row(self):
        "only one row can be returned with get"
        self.assertRaisesRegexp(ValueError, "Multiple rows returned", self.db.get, "select * from users")

    def test_no_results(self):
        "get w/ no results"
        self.assertEqual(self.db.get("select true from users where name = 'Sir Albert Einstein';"), None)

    def test_executemany(self):
        "can execute many"
        self.db.execute("truncate other.users;")
        self.db.executemany("insert into other.users (name) values (%s);", ["Mr. Smith"], ["Mr. Cramer"])
        self.assertEqual(self.db.get("select count(*) as t from other.users where name in ('Mr. Smith', 'Mr. Cramer');").t, 2)

    def test_mogrify(self):
        "can mogrify w/ inline args"
        self.assertEqual(self.db.mogrify("select true from user where email=%s;", "joe@smoe.com"),
                         b"select true from user where email='joe@smoe.com';")

    def test_mogrify_dict(self):
        "can mogrify w/ dict args"
        self.assertEqual(self.db.mogrify("select true from user where email=%(email)s;", email="joe@smoe.com"),
                         b"select true from user where email='joe@smoe.com';")

    def test_connection_from_url(self):
        "can connect from the os.getenv('DATABASE_URL')"
        db = tornpsql.Connection()
        self.assertTrue(db.get("select true as connected").connected)

    def test_adapting(self):
        "can adapt data types outside query"
        self.assertEqual(self.db.adapt("this").getquoted(), b"'this'")
        self.assertIsInstance(self.db.adapt(dict(value=10)), Json)
        self.assertIsInstance(self.db.hstore(dict(value=10)), HstoreAdapter)

    def test_raises_execptions(self):
        "can raise all psycopg2 exceptions"
        self.assertRaises(tornpsql.ProgrammingError, self.db.query, "st nothing from th;")

    def test_json(self):
        "can providing dict as an argument will adapt to json datatype"
        self.assertDictEqual(self.db.get("select %s::json as data;", dict(data="something")).data, {'data': 'something'})

    def test_hstore(self):
        "can parse hstore datatype as dict (kinda)"
        self.assertDictEqual(self.db.get("SELECT flags from users where flags is not null limit 1;").flags, dict(extra_feature='true'))

    def test_numeric_returns_decimal(self):
        "floats return Decimals"
        self.assertEqual(self.db.get("select balance from users where id = 1 limit 1;").balance, Decimal("7.10"))

    def test_execute_with_kwargs(self):
        "can query from keyword arguments"
        self.assertDictEqual(self.db.get("SELECT x from generate_series(1,10) x where x=%(id)s;", id=1), dict(x=1))
        self.assertListEqual(self.db.query("SELECT x from generate_series(1,10) x where x > %(g)s and x < %(l)s;", g=1, l=5), [{'x': 2}, {'x': 3}, {'x': 4}])

    def test_notices(self):
        "can retreive notices"
        # clear other notices
        self.db.notices
        self.db.query("set client_min_messages to NOTICE;")
        self.db.query("insert into other.users (name) values ('New Customer');")
        self.assertListEqual(self.db.notices, ["New user inserted"])
