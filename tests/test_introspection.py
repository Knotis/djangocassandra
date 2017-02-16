import datetime

from unittest import TestCase

from djangocassandra.db.meta import get_column_family

from .models import (
    SimpleTestModel,
    DateTimeTestModel,
    ComplicatedTestModel,
    PartitionPrimaryKeyModel,
    ClusterPrimaryKeyModel
)

from .util import (
    connect_db,
    create_model,
    destroy_db
)


class DatabaseIntrospectionTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        self.models = [
            SimpleTestModel,
            PartitionPrimaryKeyModel
        ]

        for m in self.models:
            create_model(
                self.connection,
                m
            )

    def tearDown(self):
        destroy_db(self.connection)

    def test_get_table_list(self):
        table_list = self.connection.introspection.get_table_list()
        self.assertIsNotNone(table_list)
        self.assertEqual(
            len(table_list),
            len(self.models)
        )

        table_names = [ti.name for ti in table_list]
        self.assertIn(
            SimpleTestModel._meta.db_table,
            table_names
        )
        self.assertIn(
            PartitionPrimaryKeyModel._meta.db_table,
            table_names
        )
