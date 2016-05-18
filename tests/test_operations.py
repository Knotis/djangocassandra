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
        from django.core.management.color import no_style

        test_model_names = ['testmodel']
        cql = self.connection.ops.sql_flush(
            no_style(),
            test_model_names,
            ()
        )

        self.assertEquals(
            2,
            len(cql)
        )
        self.assertEquals(
            'use %s;' % (self.connection.keyspace,),
            cql[0].lower()
        )
        for i in xrange(len(test_model_names)):
            self.assertEquals(
                'truncate %s;' % (test_model_names[i],),
                cql[i + 1].lower()
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
