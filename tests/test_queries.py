import warnings
import uuid

from unittest import TestCase

from .models import (
    SimpleTestModel,
    DerivedPartitionPrimaryKeyModel,
    PartitionPrimaryKeyModel,
    ClusterPrimaryKeyModel,
    ColumnFamilyTestModel
)

from .util import (
    connect_db,
    destroy_db,
    create_model
)


class DatabaseSimpleQueryTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()
        self.cached_rows = {}

        '''
        Let's create some simple data.
        '''
        create_model(
            self.connection,
            SimpleTestModel
        )

        field_names = [
            field.name if field.get_internal_type() != 'AutoField' else None
            for field in SimpleTestModel._meta.fields
        ]
        unique_value = 'bazinga'
        field_values = ['foo', 'bar', 'raw', 'awk', 'lik', 'sik', 'dik', 'doc']

        self.total_rows = 400
        for x in xrange(self.total_rows):
            test_data = {}
            i = 0
            for name in field_names:
                if not name:
                    continue

                test_data[name] = field_values[i]
                i += 1

            if unique_value:
                test_data['field_3'] = unique_value
                unique_value = None

            created_instance = SimpleTestModel.objects.create(**test_data)
            self.cached_rows[created_instance.pk] = created_instance

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_filter_on_unindexed_column(self):
        field_3_filter = SimpleTestModel.objects.filter(field_3='raw')

        expected_count = 0
        for _, o in self.cached_rows.iteritems():
            if o.field_3 == 'raw':
                expected_count += 1

        self.assertEqual(expected_count, len(field_3_filter))
        for o in field_3_filter:
            self.assertTrue(o.pk in self.cached_rows.keys())

    def test_partial_inefficient_get(self):
        field_3_get = SimpleTestModel.objects.get(field_3='bazinga')
        partial_inefficient_get = SimpleTestModel.objects.get(
            pk=field_3_get.pk,
            field_3='bazinga'
        )

        self.assertIsNotNone(partial_inefficient_get)
        self.assertTrue(partial_inefficient_get.pk in self.cached_rows.keys())

    def test_get_on_unindexed_column(self):
        field_3_get = SimpleTestModel.objects.get(field_3='bazinga')

        self.assertIsNotNone(field_3_get)
        self.assertTrue(field_3_get.pk in self.cached_rows.keys())

    def test_query_all(self):
        all_rows = list(SimpleTestModel.objects.all())

        self.assertEqual(len(all_rows), self.total_rows)

        for row in all_rows:
            cache = self.cached_rows.get(row.pk)
            fields = cache._meta.fields
            for field in fields:
                self.assertEqual(
                    getattr(cache, field.name),
                    getattr(row, field.name)
                )


class DatabaseClusteringKeyTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        self.cached_rows = {}

        '''
        Now let's create some data that is clustered
        '''
        create_model(
            self.connection,
            ClusterPrimaryKeyModel
        )

        manager = ClusterPrimaryKeyModel.objects

        self.uuid0 = str(uuid.uuid4())
        manager.create(
            field_1=self.uuid0,
            field_2='aaaa',
            field_3='bbbb',
            data='Foo'
        )

        manager.create(
            field_1=self.uuid0,
            field_2='bbbb',
            field_3='cccc',
            data='Tao'
        )

        self.uuid1 = str(uuid.uuid4())
        manager.create(
            field_1=self.uuid1,
            field_2='aaaa',
            field_3='aaaa',
            data='Bar'
        )

        manager.create(
            field_1=self.uuid1,
            field_2='bbbb',
            field_3='aaaa',
            data='Lel'
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def inefficient_filter(self):
        manager = ClusterPrimaryKeyModel.objects
        all_rows = list(manager.all())

        with warnings.catch_warnings(record=True) as w:
            filtered_rows = list(manager.filter(field_3='aaaa'))

            self.assertEqual(
                1,
                len(w)
            )

        filtered_inmem = [r for r in all_rows if r.field_3 == 'aaaa']

        self.assertEqual(
            len(filtered_rows),
            len(filtered_inmem)
        )

        for r in range(len(filtered_rows)):
            self.assertEqual(
                filtered_rows[r].data,
                filtered_inmem[r].data
            )

    def test_pk_filter(self):
        manager = ClusterPrimaryKeyModel.objects
        all_rows = list(manager.all())

        filtered_rows = list(manager.filter(field_1=self.uuid1))

        filtered_rows_inmem = [r for r in all_rows if r.pk == self.uuid1]

        self.assertEqual(
            len(filtered_rows),
            len(filtered_rows_inmem)
        )

        for i in range(len(filtered_rows)):
            self.assertEqual(
                filtered_rows[i].data,
                filtered_rows_inmem[i].data
            )

    def test_clustering_key_filter(self):
        manager = ClusterPrimaryKeyModel.objects
        all_rows = list(manager.all())

        with warnings.catch_warnings(record=True) as w:
            filtered_rows = list(manager.filter(
                field_1=self.uuid1,
                field_2='aaaa',
                field_3='aaaa'
            ))

            self.assertEqual(
                0,
                len(w)
            )

        with warnings.catch_warnings(record=True) as w:
            filtered_rows = list(manager.filter(
                field_1=self.uuid1
            ).filter(
                field_2='aaaa'
            ).filter(
                field_3='aaaa'
            ))

            self.assertEqual(
                0,
                len(w)
            )

        filtered_rows_inmem = [
            r for r in all_rows if
            r.field_1 == self.uuid1 and
            r.field_2 == 'aaaa' and
            r.field_3 == 'aaaa'
        ]

        self.assertEqual(
            len(filtered_rows),
            len(filtered_rows_inmem)
        )

        for i in range(len(filtered_rows)):
            self.assertEqual(
                filtered_rows[i].data,
                filtered_rows_inmem[i].data
            )

    def test_orderby(self):
        manager = ClusterPrimaryKeyModel.objects
        filtered_rows = list(manager.filter(field_1=self.uuid1))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')

            filtered_rows_ordered = list(
                manager.filter(
                    field_1=self.uuid1
                ).order_by('field_2')
            )

            filtered_rows_ordered_desc = list(
                manager.filter(
                    field_1=self.uuid1
                ).order_by('-field_2')
            )

            self.assertEqual(
                0,
                len(w)
            )

        filtered_rows.sort(
            key=lambda x: x.field_2,
            reverse=False
        )

        for i in range(len(filtered_rows)):
            self.assertEqual(
                filtered_rows[i].data,
                filtered_rows_ordered[i].data
            )

        filtered_rows.sort(
            key=lambda x: x.field_2,
            reverse=True
        )

        for i in range(len(filtered_rows)):
            self.assertEqual(
                filtered_rows[i].data,
                filtered_rows_ordered_desc[i].data
            )


class DatabasePartitionKeyTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        self.cached_rows = {}

        '''
        Now let's create some data that is clustered
        '''
        create_model(
            self.connection,
            PartitionPrimaryKeyModel
        )

        manager = PartitionPrimaryKeyModel.objects
        manager.create(
            field_1='aaaa',
            field_2='aaaa',
            field_3='bbbb',
            field_4='cccc',
            data='Foo'
        )

        manager.create(
            field_1='aaaa',
            field_2='bbbb',
            field_3='cccc',
            field_4='dddd',
            data='Tao'
        )

        manager.create(
            field_1='bbbb',
            field_2='aaaa',
            field_3='aaaa',
            field_4='eeee',
            data='Bar'
        )

        manager.create(
            field_1='bbbb',
            field_2='bbbb',
            field_3='aaaa',
            field_4='ffff',
            data='Lel'
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_in_filter(self):
        qs = PartitionPrimaryKeyModel.objects.filter(pk__in=[
            'aaaa',
            'bbbb'
        ])
        self.assertEqual(4, len(qs))

    def test_filter_all_partition_keys(self):
        qs = PartitionPrimaryKeyModel.objects.filter(
            field_1='aaaa',
            field_2='bbbb'
        )
        self.assertEqual(1, len(qs))


class DerivedPartitionKeyModelTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        self.cached_rows = {}

        '''
        Now let's create some data that is clustered
        '''
        create_model(
            self.connection,
            DerivedPartitionPrimaryKeyModel
        )

        manager = DerivedPartitionPrimaryKeyModel.objects
        manager.create(
            field_1='aaaa',
            field_2='aaaa',
            inherited_1='bbbb',
            inherited_2='cccc',
            data='Foo'
        )

        manager.create(
            field_1='aaaa',
            field_2='bbbb',
            inherited_1='cccc',
            inherited_2='dddd',
            data='Tao'
        )

        manager.create(
            field_1='bbbb',
            field_2='aaaa',
            inherited_1='aaaa',
            inherited_2='eeee',
            data='Bar'
        )

        manager.create(
            field_1='bbbb',
            field_2='bbbb',
            inherited_1='aaaa',
            inherited_2='ffff',
            data='Lel'
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_nothing(self):
        pass


class ColumnFamilyModelPagingQueryTestCase(TestCase):
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
            field.name if field.get_internal_type() != 'AutoField' and
            field.db_column != 'pk__token' else None
            for field in ColumnFamilyTestModel._meta.fields
        ]
        field_values = ['foo', 'bar', 'raw', 'awk', 'lik', 'sik', 'dik', 'doc']

        self.total_rows = 10
        self.created_rows = 0
        for x in xrange(self.total_rows):
            test_data = {}
            for name in field_names:
                if not name:
                    continue

                test_data[name] = field_values[x % len(field_values)]

            if test_data['field_1'] in self.cached_rows:
                continue

            created_instance = ColumnFamilyTestModel.objects.create(
                **test_data
            )
            self.cached_rows[created_instance.pk] = created_instance
            self.created_rows += 1

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_paged_query(self):
        all_results = []
        one_result = ColumnFamilyTestModel.objects.all()[:1]
        self.assertEqual(len(one_result), 1)
        all_results.extend(one_result)
        next_result = one_result.next()
        self.assertEqual(len(next_result), 1)
        all_results.extend(next_result)

        self.assertNotEqual(
            one_result[0].pk,
            next_result[0].pk
        )
        self.assertNotEqual(
            one_result[0].field_2,
            next_result[0].field_2
        )
        self.assertNotEqual(
            one_result[0].field_3,
            next_result[0].field_3
        )

        for i in xrange(self.created_rows + 10):
            next_result = next_result.next()
            if not len(next_result):
                break

            all_results.extend(next_result)

        self.assertEqual(len(all_results), self.created_rows)
