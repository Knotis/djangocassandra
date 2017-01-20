from unittest import TestCase

from .util import (
    connect_db,
    destroy_db
)


class CommandsTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_migration(self):
        from django.core.management import call_command
        call_command('makemigrations')
        call_command('migrate')
