from unittest import TestCase

from .util import (
    connect_db,
    destroy_db
)


class ConnectionTestCase(TestCase):
    def setUp(self):
        self.connection = None

        import django
        django.setup()

    def tearDown(self):
        if None is not self.connection:
            destroy_db(self.connection)

    def test_connection(self):
        self.connection = connect_db()

        self.assertIsNotNone(self.connection)


class ConnectionCapabilitiesTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_create_cursor(self):
        cursor = self.connection.create_cursor()
        self.assertIsNotNone(cursor)

        cursor2 = self.connection._cursor()
        self.assertIsNotNone(cursor2)

    def test_get_current_keyspace(self):
        keyspace = self.connection.settings_dict.get('DEFAULT_KEYSPACE')
        self.assertEqual(
            keyspace,
            self.connection.current_keyspace()
        )
