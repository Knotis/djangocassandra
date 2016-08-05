import warnings
from random import randint
from unittest import TestCase

from .models import (
    ColumnFamilyTestModel,
    ColumnFamilyIndexedTestModel,
    ClusterPrimaryKeyModel,
    ForeignPartitionKeyModel,
    DictFieldModel
)

from .util import (
    connect_db,
    destroy_db,
    create_model
)


class ColumnFamilyModelTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()
        self.cached_rows = {}

        '''
        Let's create some simple data.
        '''
        create_model(
            self.connection,
            ColumnFamilyTestModel
        )

        field_names = [
            field.name if field.get_internal_type() != 'AutoField' else None
            for field in ColumnFamilyTestModel._meta.fields
        ]
        field_values = ['foo', 'bar', 'raw', 'awk', 'lik', 'sik', 'dik', 'doc']

        self.total_rows = 100
        value_index = 0
        for x in xrange(self.total_rows):
            test_data = {}
            for name in field_names:
                if not name:
                    continue

                test_data[name] = field_values[value_index % len(field_values)]
                value_index += 1

            test_data['field_1'] = test_data['field_1'] + str(
                randint(1000, 9999)
            )

            if test_data['field_1'] in self.cached_rows.keys():
                continue

            created_instance = ColumnFamilyTestModel.objects.create(
                **test_data
            )
            self.cached_rows[created_instance.pk] = created_instance

        self.created_instances = len(self.cached_rows)

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_token_partition_key_field_value_to_string(self):
        first_instance = ColumnFamilyTestModel.objects.all()[:1][0]

        token_field, _, _, _ = ColumnFamilyTestModel._meta.get_field_by_name(
            'pk_token'
        )

        result = token_field.value_to_string(first_instance)
        self.assertIsNotNone(result)


class ColumnFamilyTestIndexedQueriesTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()
        self.cached_rows = {}

        '''
        Let's create some simple data.
        '''
        create_model(
            self.connection,
            ColumnFamilyIndexedTestModel
        )

        field_names = [
            field.name if field.get_internal_type() != 'AutoField' else None
            for field in ColumnFamilyIndexedTestModel._meta.fields
        ]
        field_values = [
            'foo',
            'bar',
            'raw',
            'awk',
            'lik',
            'sik',
            'dik',
            'doc',
            'dab'
        ]
        high_cardinality_field_values = ['yes', 'no']

        self.total_rows = 400
        value_index = 0
        for x in xrange(self.total_rows):
            test_data = {}
            for name in field_names:
                if not name:
                    continue

                test_data[name] = field_values[value_index % len(field_values)]
                test_data['field_4'] = (
                    high_cardinality_field_values[
                        value_index % len(
                            high_cardinality_field_values
                        )
                    ]
                )
                value_index += 1

            test_data['field_1'] = test_data['field_1'] + str(
                randint(1000, 9999)
            )

            if test_data['field_1'] in self.cached_rows.keys():
                continue

            created_instance = ColumnFamilyIndexedTestModel.objects.create(
                **test_data
            )
            self.cached_rows[created_instance.pk] = created_instance

        self.created_instances = len(self.cached_rows)

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_partial_inefficient_get_query(self):
        all_results = ColumnFamilyIndexedTestModel.objects.all()
        all_results = [x for x in all_results]
        last_result = all_results[-1]
        last_result.field_3 = 'tool'
        last_result_indexed_value = last_result.field_4
        last_result.save()

        partial_inefficient_get = (
            ColumnFamilyIndexedTestModel.objects.get(
                field_3='tool',
                field_4=last_result_indexed_value
            )
        )

        self.assertIsNotNone(partial_inefficient_get)
        self.assertTrue(partial_inefficient_get.pk in self.cached_rows.keys())


class ForeignPartitionKeyModelTestCase(TestCase):
    def setUp(self):
        import django
        django.setup()

        self.connection = connect_db()

        create_model(
            self.connection,
            ClusterPrimaryKeyModel
        )
        create_model(
            self.connection,
            ForeignPartitionKeyModel
        )

    def tearDown(self):
        destroy_db(self.connection)

    def test_order_by_efficient(self):
        rel_instance = ClusterPrimaryKeyModel()
        rel_instance.auto_populate()
        rel_instance.save()

        instances = []
        for i in xrange(10):
            instances.append(ForeignPartitionKeyModel.objects.create(
                related=rel_instance
            ))

        with warnings.catch_warnings(record=True) as w:
            ordered_query = ForeignPartitionKeyModel.objects.filter(
                related=rel_instance
            ).order_by('-created')

            results = list(ordered_query)

            self.assertEqual(
                0,
                len(w)
            )
            self.assertEqual(
                10,
                len(results)
            )


class TestDictFieldModel(TestCase):
    def setUp(self):
        import django
        django.setup()

        self.connection = connect_db()

        create_model(
            self.connection,
            DictFieldModel
        )

    def tearDown(self):
        destroy_db(self.connection)

    def test_creation(self):
        instance = DictFieldModel.objects.create(
            parameters={'key0': 'value0', 'key1': 'value1'}
        )

        self.assertIsNotNone(instance)
