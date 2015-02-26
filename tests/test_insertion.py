import datetime

from unittest import TestCase

from uuid import UUID

from djangocassandra.db.models import get_column_family

from .models import (
    SimpleTestModel,
    DateTimeTestModel,
    ComplicatedTestModel
)

from .util import (
    connect_db,
    create_model,
    destroy_db
)


class DatabaseInsertionTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        create_model(
            self.connection,
            SimpleTestModel
        )

        create_model(
            self.connection,
            DateTimeTestModel
        )

        create_model(
            self.connection,
            ComplicatedTestModel
        )

    def tearDown(self):
        destroy_db(self.connection)

    def test_insertion(self):
        inserted = SimpleTestModel.objects.create(
            field_1='foo',
            field_2='bar',
            field_3='raw'
        )

        self.assertTrue(
            isinstance(inserted.pk, UUID)
        )

        self.assertEqual('foo', inserted.field_1)
        self.assertEqual('bar', inserted.field_2)
        self.assertEqual('raw', inserted.field_3)

        cf_simple_model = get_column_family(
            self.connection,
            SimpleTestModel
        )

        result = cf_simple_model.objects.get(id=inserted.pk)

        self.assertEqual(result.pk, inserted.pk)
        self.assertEqual(result.field_1, inserted.field_1)
        self.assertEqual(result.field_2, inserted.field_2)
        self.assertEqual(result.field_3, inserted.field_3)

    def test_datetime_field_model(self):
        datetime_instance = DateTimeTestModel.objects.create(
            datetime_field=datetime.datetime.now()
        )
        self.assertIsNotNone(datetime_instance)

    def test_complicated_insertion(self):
        instance = ComplicatedTestModel()
        instance.auto_populate()
        instance.save()
        self.assertIsNotNone(instance)
        self.assertIsNotNone(instance.pk)
