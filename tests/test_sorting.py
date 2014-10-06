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

fake_data = [('a', 'b', 'c'), ('d', 'e', 'f'), ('g', 'h', 'i')]
class SortingTestModel(Model):
    field_1 = CharField(max_length=50)
    field_2 = CharField(max_length=50)
    field_3 = CharField(max_length=50)
    
STM = SortingTestModel
    
class TestOrderingCase(TestCase)
    query = CassandraQuery(SQLCompiler, 
                    [STM.field_1, STM.field_2, STM.field_3])
                    
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

class TestFullMatchOrdering(TestOrderingCase):
    """
        This case tests the situation in which the ordering request
        matches with the data storage ordering, is exactly one fields,
        and no sorting is needed.
    """
    
    def test_correct_sort_application(self):
        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            try:
                self.query.orderby(['field_1'])
                if self.query.can_order_efficiently:
                    assert True
                else:
                    assert False
            except InefficientQueryWarning:
                assert False
        raise

    def test_correct_sort_results(self):
        self.query.orderby(['field_1'])
        results = self.query._get_query_results()

        # Test data already sorted by field_1.
        ### COMPARE HERE.


class TestPartialMatchOrdering(TestOrderingCase):
    """
        This case tests the situation in which the first of the ordering
        fields matches with the ordering of the stored data.
        
        Currently, this case is not implemented in the sorting, as
        multiple search terms automatically make it inefficient and
        trigger a full sort.
    """
    
    def test_correct_sort_application(self):
        pass

    def test_correct_sort_results(self):
        pass


class TestDefaultOrdering(TestOrderingCase):
    """
        This case tests that the unaccelerated ordering works and raises
        the correct warning about slow execution.
    """

    def test_for_warnings(self):
        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            try:
                self.query.orderby(['field_2'])
                assert False
            except InefficientQueryWarning:
                assert True
        raise

    def test_correct_sort_results(self):
        self.query.orderby(['field_2', 'field_3', 'field_1'])
        ordering = self.query.ordering
        local_cache = None
        local_cache = sort_rows(local_cache, ordering) #<--- Not the right thing.
        
        ### SORT DATA HERE. 2, 3, 1 ordering.
        
        ### COMPARE HERE.
