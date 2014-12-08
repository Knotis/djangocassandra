"""
    This file tests the sorting performance of the djangocassandra back end.

    Tests are implemented for the following features:
        - Verifying that no sort is applied to already sorted data, when
          properly queried.
        - Verifying that this data is properly sorted.
        - Verifying that only minimal sorting is applied to partially
           sorted data, when properly queried.
        - Verifying that this data is properly sorted.
        - Verify that the djangocassandra backend raises proper warnings
          for unaccelerated sorts.
        - Verifying that the djangocassandra correctly sorts data in the
          unaccelerated case.
"""

import warnings

from django.conf import settings

from django.db.models import (
    Model,
    CharField
)

from djangocassandra.db.backends.cassandra.base import (
    CassandraQuery,
    SQLCompiler
)

from djangocassandra.db.backends.cassandra.utils import (
    sort_rows
)

from unittest import TestCase

from util import (
    connect_db,
    create_db,
    populate_db,
    destroy_db
)

from djangocassandra.db.backends.cassandra.compiler import (
    InefficientQueryWarning,
)

fake_data = [('a', 'b', 'c'), ('d', 'e', 'f'), ('g', 'h', 'i')]
class SortingTestModel(Model):
    field_1 = CharField(max_length=50)
    field_2 = CharField(max_length=50)
    field_3 = CharField(max_length=50)

class SortingTestCase(TestCase):
    connection = None

    def setUp(self):
        try:
            try:
                self.connection = connect_db()
            except Exception, e:
                self.connection = None
                raise e
            create_db(self.connection, SortingTestModel)
            test_data = []
            for x, y, z in fake_data:
                test_data.append(SortingTestModel(field_1=x, field_2=y, field_3=z))
            populate_db(self.connection, test_data)
        except Exception, e:
            self.tearDown()
            raise e

    def tearDown(self):
        destroy_db(self.connection)

class TestFullOrderingMatch(SortingTestCase):
    def test_correct_sort_application(self):
        assert True

    def test_correct_sort_results(self):
        assert True


class TestPartialOrderingMatch(SortingTestCase):
    def test_correct_sort_application(self):
        assert True

    def test_correct_sort_results(self):
        assert True


class TestDefaultSort(SortingTestCase):
    def test_for_warnings(self):
        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            try:
                # DO SLOW SORT HERE
                assert False
            except InefficientQueryWarning:
                assert True
        raise 

    def test_correct_sort_results(self):
        assert True
