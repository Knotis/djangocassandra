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

        create_model(
            self.connection,
            PartitionPrimaryKeyModel
        )

        create_model(
            self.connection,
            ClusterPrimaryKeyModel
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_insertion(self):
        inserted = SimpleTestModel.objects.create(
            field_1='foo',
            field_2='bar',
            field_3='raw'
        )

        self.assertIsNotNone(
            inserted.pk
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

        pk = result.pk
        result.delete()

        try:
            should_not_exist = cf_simple_model.objects.get(
                id=pk
            )
            self.assertIsNone(should_not_exist)

        except cf_simple_model.DoesNotExist:
            pass

    def test_datetime_field_model(self):
        datetime_instance = DateTimeTestModel.objects.create(
            datetime_field=datetime.datetime.now()
        )
        self.assertIsNotNone(datetime_instance)

    def test_complicated_insertion_and_delete(self):
        instance = ComplicatedTestModel()
        instance.auto_populate()
        instance.save()
        self.assertIsNotNone(instance)
        self.assertIsNotNone(instance.pk)

        pk = instance.pk
        instance.delete()

        try:
            should_not_exist = ComplicatedTestModel.objects.get(
                pk=pk
            )
            self.assertIsNone(should_not_exist)

        except ComplicatedTestModel.DoesNotExist:
            pass

    def test_partition_key_test_model(self):
        instance = PartitionPrimaryKeyModel()
        instance.auto_populate()
        instance.save()
        self.assertIsNotNone(instance)
        self.assertIsNotNone(instance.pk)

    def test_clustering_key_test_model(self):
        instance = ClusterPrimaryKeyModel()
        instance.auto_populate()
        instance.save()
        self.assertIsNotNone(instance)
        self.assertIsNotNone(instance.pk)
