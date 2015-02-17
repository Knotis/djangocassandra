from unittest import TestCase

from .models import (
    SimpleTestModel
)

from .util import (
    connect_db,
    destroy_db,
    create_model
)


class DatabaseSimpleQueryTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        create_model(
            self.connection,
            SimpleTestModel
        )

        field_names = [
            field.name if field.get_internal_type() != 'AutoField' else None
            for field in SimpleTestModel._meta.fields
        ]
        field_values = ['foo', 'bar', 'raw', 'awk', 'lik', 'sik', 'dik', 'doc']

        self.total_records = 10
        for x in xrange(self.total_records):
            test_data = {}
            i = 0
            for name in field_names:
                if not name:
                    continue

                test_data[name] = field_values[i]
                i += 1

            SimpleTestModel.objects.create(**test_data)
        
    def tearDown(self):
        destroy_db(self.connection)

    def test_query_all(self):
        all_records = list(SimpleTestModel.objects.all())

        self.assertEqual(len(all_records), self.total_records)
        
