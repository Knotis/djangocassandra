import warnings

from uuid import uuid4

from collections import OrderedDict

from django.db.utils import (
    ProgrammingError,
    NotSupportedError
)

from django.db.models.sql.constants import MULTI
from django.db.models.sql.where import (
    WhereNode,
    AND,
    OR
)

from djangotoolbox.db.basecompiler import (
    NonrelQuery,
    NonrelCompiler,
    NonrelInsertCompiler,
    NonrelUpdateCompiler,
    NonrelDeleteCompiler
)

from cqlengine.query import (
    BatchQuery,
    QueryException
)

from djangocassandra.db.models import (
    get_column_family
)

from .exceptions import InefficientQueryError
from .predicate import (
    CompoundPredicate,
    COMPOUND_OP_AND,
    COMPOUND_OP_OR
)

from .utils import (
    safe_call,
    sort_rows
)


class CassandraQuery(NonrelQuery):
    MAX_RESULT_COUNT = 10000

    def __init__(
        self,
        compiler,
        fields
    ):
        super(CassandraQuery, self).__init__(
            compiler,
            fields
        )

        self.meta = self.query.get_meta()
        
        if hasattr(self.query.model, 'CassandraMeta'):
            self.cassandra_meta = self.query.model.CassandraMeta

        else:
            self.cassandra_meta = None

        self.pk_column = self.meta.pk.column
        self.column_family = self.meta.db_table
        self.columns = fields
        self.where = None
        self.cache = None
        self.allows_inefficient = True  # TODO: Make this a config setting
        self.inefficient_filtering = []
        self.inefficient_ordering = []

        self.connection.ensure_connection()
        self.column_family_class = get_column_family(
            self.connection,
            self.query.model
        )

        self.column_names = [
            field.db_column if field.db_column else field.column
            for field in fields
        ]
        self.indexed_columns = [
            field.db_column if field.db_column else field.column
            for field in fields if field.db_index
        ]
        self.cql_query = self.column_family_class.objects.values_list(
            *self.column_names
        ).all()

    def _get_rows_by_indexed_column(self, range_predicate):
        if range_predicate._is_exact():
            rows = self.cql_query.filter(**{
                range_predicate.column: range_predicate.start
            })

        else:
            if None is range_predicate.start:
                start_op = None

            else:
                if range_predicate.start_inclusive:
                    start_op = 'gte'
                else:
                    start_op = 'gt'
                start_op = '__'.join([
                    range_predicate.column,
                    start_op
                ])

            if None is range_predicate.end:
                end_op = None

            else:
                if range_predicate.end_inclusive:
                    end_op = 'lte'
                else:
                    end_op = 'lt'
                end_op = '__'.join([
                    range_predicate.column,
                    end_op
                ])

            query = self.cql_query
            if None is not start_op:
                query = query.filter(**{
                    start_op: range_predicate.start
                })
    
            if None is not end_op:
                query = query.filter(**{
                    end_op: range_predicate.end
                })

            rows = [query]
                
        return rows
    
    def get_row_range(self, range_predicate):
        pk_column = self.query.get_meta().pk.column

        '''
        !!! Does this need to check for clustering key? !!!
        '''
        assert(
            range_predicate.column == pk_column or
            range_predicate.column in self.indexed_columns
        )

        return self._get_rows_by_indexed_column(range_predicate)
    
    def get_all_rows(self):
         return self.cql_query
    
    def _get_query_results(self):
        if self.cache == None:
            assert(self.root_predicate != None)

            self.cache = self.root_predicate.get_matching_rows(self)

            if self.inefficient_ordering:
                for ordering in self.inefficient_ordering:
                    sort_rows(self.cache, ordering)

        return self.cache
    
    @safe_call
    def fetch(self, low_mark, high_mark):
        if self.root_predicate == None:
            raise DatabaseError('No root query node')
        
        if high_mark is not None and high_mark <= low_mark:
            return

        results = self._get_query_results()

        if low_mark is not None or high_mark is not None:
            results = results[low_mark:high_mark]

        

        for entity in results:
            yield OrderedDict(zip(self.column_names, entity))

    def count(
        self,
        limit=None
    ):
        return self.cql_query.count()

    def delete(
        self,
        columns=set()
    ):
        return self.cql_query.delete()

    def order_by(
        self,
        ordering
    ):
        if isinstance(ordering, bool):
            self.reverse_order = not ordering
            return

        for order in ordering:
            if isinstance(order, basestring):
                if order.startswith('-'):
                    field_name = order[1:]
                    ascending = False

                else:
                    field_name = order
                    ascending = True

            elif 2 == len(order):
                field, ascending = order
                field_name = field.name

            else:
                raise ProgrammingError(
                    'Invalid ordering specification: %s' % order,
                )

            order_string = ''.join([
                '' if ascending else '-',
                field_name
            ])

            clustering_key_fields = []
            if hasattr(self.query.model, 'CassandraMeta'):
                clustering_key_fields = (
                    self.query.model.CassandraMeta.clustering_key
                )

            valid_order_field_index = len(self.ordering) + 1
            if field_name != clustering_key_fields[valid_order_field_index]:
                if not self.allows_inefficient:
                    raise InefficientQueryError(self.cql_query)

                warnings.warn(InefficientQueryError.message)
                self.add_inefficient_order_by(order_string)
                return

            self.cql_query = self.cql_query.order_by(order_string)
            self.ordering.append(order_string)

    @safe_call
    def add_inefficient_order_by(self, ordering):
        for order in ordering:
            if order.startswith('-'):
                field_name = order[1:]
                reversed = True
            else:
                field_name = order
                reversed = False

        self.ordering_spec.append((field_name, reversed))
            
    def init_predicate(self, parent_predicate, node):
        if isinstance(node, WhereNode):
            if node.connector == OR:
                compound_op = COMPOUND_OP_OR
            elif node.connector == AND:
                compound_op = COMPOUND_OP_AND
            else:
                raise InvalidQueryOpException()
            predicate = CompoundPredicate(compound_op, node.negated)
            for child in node.children:
                child_predicate = self.init_predicate(predicate, child)
            if parent_predicate:
                parent_predicate.add_child(predicate)
        else:
            column, lookup_type, db_type, value = self._decode_child(node)
            db_value = self.convert_value_for_db(db_type, value)
            assert parent_predicate
            parent_predicate.add_filter(column, lookup_type, db_value)
            predicate = None
            
        return predicate
    
    # FIXME: This is bad. We're modifying the WhereNode object that's passed in to us
    # from the Django ORM. We should do the pruning as we build our predicates, not
    # munge the WhereNode.
    def remove_unnecessary_nodes(self, node, retain_root_node):
        if isinstance(node, WhereNode):
            child_count = len(node.children)
            for i in range(child_count):
                node.children[i] = self.remove_unnecessary_nodes(node.children[i], False)
            if (not retain_root_node) and (not node.negated) and (len(node.children) == 1):
                node = node.children[0]
        return node
        
    @safe_call
    def add_filters(self, filters):
        """
        Traverses the given Where tree and adds the filters to this query
        """
        
        assert isinstance(filters,WhereNode)
        self.remove_unnecessary_nodes(filters, True)
        self.root_predicate = self.init_predicate(None, filters)


class SQLCompiler(NonrelCompiler):
    query_class = CassandraQuery

    def execute_sql(
        self,
        result_type=MULTI
    ):
        return super(SQLCompiler, self).execute_sql(result_type)


class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    def insert(
        self,
        values,
        return_id
    ):
        meta = self.query.get_meta()

        column_family = get_column_family(
            self.connection,
            self.query.model
        )

        inserted_row_keys = []

        with BatchQuery() as b:
            for row in values:
                # HELP!:Is this the right place for this logic? I can't seem
                # to figure out where Django adds the value for auto fields.
                if meta.has_auto_field and meta.pk.column not in row.keys():
                    row[meta.pk.column] = uuid4()

                inserted = column_family.batch(b).create(
                    **row
                )

                inserted_row_keys.append(inserted.pk)

        if return_id:
            if len(inserted_row_keys) == 1:
                return inserted_row_keys[0]

            else:
                return inserted_row_keys


class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    def update(
        self,
        values
    ):
        return NonrelUpdateCompiler.update(self, values)


class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass
