from unittest import TestCase

from .util import (
    connect_db,
    destroy_db,
    create_model
)

from .models import SimpleTestModel


class DatabaseCreationTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

    def tearDown(self):
        destroy_db(self.connection)

    def test_table_creation(self):
        create_model(
            self.connection,
            SimpleTestModel
        )
