from random import randint
from unittest import TestCase

from .models import (
    ColumnFamilyTestModel
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
