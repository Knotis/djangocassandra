import uuid

from unittest import TestCase

from .models import (
    ColumnFamilyTestModel
)

from .util import (
    connect_db,
    create_model,
    destroy_db
)


class OperationsTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        create_model(
            self.connection,
            ColumnFamilyTestModel
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_flush(self):
        try:
            self.connection.ops.sql_flush(
                None, [
                    'ColumnFamilyTestModel'
                ],
                None
            )
            self.assertTrue(False)

        except Exception, e:
            self.assertEqual(
                e.message,
                'Not Implemented'
            )

    def test_value_to_db_auto(self):
        value = uuid.uuid4()
        self.assertEquals(
            self.connection.ops.value_to_db_auto(value).hex,
            value.hex
        )

        self.assertEquals(
            self.connection.ops.value_to_db_auto(value.hex).hex,
            value.hex
        )
