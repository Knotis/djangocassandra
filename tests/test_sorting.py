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

from django.conf import settings

from django.db.models import (
    Model,
    CharField
)

from unittest import TestCase

from util import (
    connect_db,
    create_db,
    populate_db,
    destroy_db
)

fake_data = [('a', 'b', 'c'), ('d', 'e', 'f'), ('g', 'h', 'i')]
class SortingTestModel(Model):
    field_1 = CharField(max_length=50)
    field_2 = CharField(max_length=50)
    field_3 = CharField(max_length=50)

class TestFullOrderingMatch(TestCase):
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
                test_data.append(SortingTestModel(x, y, z))
            populate_db(self.connection, test_data)
        except Exception, e:
            self.tearDown()
            raise e

    def tearDown(self):
        destroy_db(self.connection)

    def test_correct_sort_application(self):
        pass

    def test_correct_sort_results(self):
        pass


class TestPartialOrderingMatch(TestCase):
    def test_correct_sort_application(self):
        pass

    def test_correct_sort_results(self):
        pass


class TestDefaultSort(TestCase):

    def test_for_warnings(self):
        pass

    def test_correct_sort_results(self):
        pass
