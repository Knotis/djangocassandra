import re
import itertools

from django.db.utils import (
    ProgrammingError
)

from django.db.models import ForeignKey
from django.db.models.sql.constants import MULTI
from django.db.models.sql.datastructures import EmptyResultSet
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

from cassandra.cqlengine.query import BatchQuery

from djangocassandra.db.meta import (
    get_column_family
)
from djangocassandra.db.fields import (
    TokenPartitionKeyField
)

from .predicate import (
    CompoundPredicate,
    COMPOUND_OP_AND,
    COMPOUND_OP_OR
)

from .exceptions import InvalidQueryOpException

from .utils import (
    safe_call
)


class CassandraQuery(NonrelQuery):
    def __init__(
        self,
        compiler,
        fields,
        allows_inefficient=None
    ):
        super(CassandraQuery, self).__init__(
            compiler,
            fields
        )

        self.meta = self.query.get_meta()

        if hasattr(self.query.model, 'Cassandra'):
            self.cassandra_meta = self.query.model.Cassandra

            if (
                None is allows_inefficient and
                hasattr(self.cassandra_meta, 'allow_inefficient_queries')
            ):
                self.allows_inefficient = (
                    self.cassandra_meta.allow_inefficient_queries
                )

        else:
            self.cassandra_meta = None

        if None is allows_inefficient:
            if 'ALLOW_INEFFICIENT_QUERIES' in self.connection.settings_dict:
                self.allows_inefficient = self.connection.settings_dict[
                    'ALLOW_INEFFICIENT_QUERIES'
                ]

            else:
                self.allows_inefficient = True  # Default to True

        self.pk_column = (
            self.meta.pk.db_column
            if self.meta.pk.db_column
            else self.meta.pk.column
        )
        self.column_family = self.meta.db_table

        self.columns = []
        model_fields = self.meta.fields
        for f in model_fields:
            if isinstance(f, TokenPartitionKeyField):
                continue

            self.columns.append(f)

        self.where = None
        self.limit = 100000000
        self.timeout = None  # TODO: Make this a config setting
        self.cache = None
        self.ordering = []
        self.filters = []
        self.inefficient_ordering = []

        self.high_mark = None
        self.low_mark = None

        self.connection.ensure_connection()
        self.column_family_class = get_column_family(
            self.connection,
            self.query.model
        )

        self.column_names = [
            column.db_column if column.db_column else column.column
            for column in self.columns
        ]
        self.indexed_columns = [
            column.db_column if column.db_column else column.column
            for column in self.columns if column.db_index
        ]

        self.partition_columns = [self.pk_column]
        if hasattr(self.cassandra_meta, 'partition_keys'):
            for key in self.cassandra_meta.partition_keys:
                field = self.meta.get_field(key)
                self.partition_columns.append(
                    field.db_column
                    if field.db_column
                    else field.column
                )

        self.clustering_columns = []
        if hasattr(self.cassandra_meta, 'clustering_keys'):
            for key in self.cassandra_meta.clustering_keys:
                field = self.meta.get_field(key)
                self.clustering_columns.append(
                    field.db_column
                    if field.db_column
                    else field.column
                )

        self.cql_query = self.column_family_class.objects.values_list(
            *self.column_names
        ).allow_filtering()

    @property
    def cassandra_pk_columns(self):
        pk_columns = []
        if self.partition_columns and len(self.partition_columns):
            for column in self.partition_columns:
                pk_columns.append(column)

            if self.clustering_columns and len(self.clustering_columns):
                for column in self.clustering_columns:
                    pk_columns.append(column)

        else:
            pk_columns.append(self.pk_column)

        return pk_columns

    @property
    def filterable_columns(self):
        return itertools.chain(
            ['pk__token'],
            self.partition_columns,
            self.clustering_columns,
            self.indexed_columns
        )

    def _get_rows_by_indexed_column(self, range_predicates):
        # Let's sort the predicates in efficient order.
        sorted_predicates = []
        indexed_predicates = []
        predicates_by_column = {
            predicate.column: predicate for
            predicate in range_predicates
        }

        for column_name in self.partition_columns:
            if column_name in predicates_by_column:
                sorted_predicates.append(
                    predicates_by_column[column_name]
                )
                del predicates_by_column[column_name]

        for column_name in self.clustering_columns:
            if column_name in predicates_by_column:
                sorted_predicates.append(
                    predicates_by_column[column_name]
                )
                del predicates_by_column[column_name]

        if 'pk__token' in predicates_by_column:
            sorted_predicates.append(
                predicates_by_column['pk__token']
            )

        for column_name in self.indexed_columns:
            if column_name in predicates_by_column:
                indexed_predicates.append(
                    predicates_by_column[column_name]
                )
                del predicates_by_column[column_name]

        assert ((
            len(sorted_predicates) +
            len(indexed_predicates)
        ) == len(range_predicates))

        def filter_range(query, predicate):
            if predicate._is_exact():
                return query.filter(**{
                    predicate.column: predicate.start
                })

            else:
                if None is predicate.start:
                    start_op = None

                else:
                    if predicate.start_inclusive:
                        start_op = 'gte'
                    else:
                        start_op = 'gt'
                    start_op = '__'.join([
                        predicate.column,
                        start_op
                    ])

                if None is predicate.end:
                    end_op = None

                else:
                    if predicate.end_inclusive:
                        end_op = 'lte'
                    else:
                        end_op = 'lt'
                    end_op = '__'.join([
                        predicate.column,
                        end_op
                    ])

                filter_ops = {}
                if None is not start_op:
                    filter_ops.update({
                        start_op: predicate.start
                    })

                if None is not end_op:
                    filter_ops.update({
                        end_op: predicate.end
                    })

                return query.filter(**filter_ops)

        for predicate in sorted_predicates:
            self.cql_query = filter_range(
                self.cql_query,
                predicate
            )

        for predicate in indexed_predicates:
            self.cql_query = filter_range(
                self.cql_query,
                predicate
            )

        return self.cql_query

    def get_row_range(self, range_predicates):
        '''
        !!! Does this need to check for clustering key? !!!
        '''
        if not isinstance(range_predicates, list):
            range_predicates = list(range_predicates)

        for predicate in range_predicates:
            assert(
                predicate.column in self.filterable_columns
            )

        query = self._get_rows_by_indexed_column(range_predicates)
        return query

    def get_all_rows(self):
        return self._get_query_results()

    def _get_query_results(self):
        if None is self.cache:
            assert(None is not self.root_predicate)
            self.cache = self.root_predicate.get_matching_rows(self)

        return self.cache

    @safe_call
    def fetch(self, low_mark, high_mark):

        if None is self.root_predicate:
            raise Exception('No root query node')

        self.low_mark = low_mark
        self.high_mark = high_mark

        if (
            high_mark is not None and
            low_mark is not None and
            high_mark < low_mark
        ):
            raise Exception('Can\'t slice query high_mark > low_mark')

        if (
            not low_mark and high_mark and
            self.root_predicate.can_evaluate_efficiently(
                self.partition_columns,
                self.clustering_columns,
                self.indexed_columns
            )
        ):
            self.limit = high_mark

        if None is not self.limit:
            self.cql_query = self.cql_query.limit(self.limit)

        results = self._get_query_results()

        for entity in results:
            yield entity

    def count(
        self,
        limit=None
    ):
        return len(
            [x for x in self.root_predicate.get_matching_rows(self)]
        )

    def delete(
        self,
        columns=set()
    ):
        if self.root_predicate.can_evaluate_efficiently(
            self.partition_columns,
            self.clustering_columns,
            self.indexed_columns
        ):
            self.root_predicate.get_matching_rows(self)
            for r in self.cql_query:
                r.delete()

        else:
            rows = self.root_predicate.get_matching_rows(self)
            for row in rows:
                self.column_family_class.get(**{
                    field: row[field] for field in self.partition_columns
                }).delete()

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

            partition_key_filtered = True
            for partition_key in self.partition_columns:
                found = False
                for filter_tuple in self.filters:
                    field = filter_tuple[0]
                    if isinstance(field, ForeignKey):
                        partition_key = re.sub(
                            '_id$',
                            '',
                            partition_key
                        )

                    if partition_key == field.name:
                        found = True
                        continue

                if not found:
                    partition_key_filtered = False
                    break

            if (
                partition_key_filtered
                and field_name in self.clustering_columns
                and not self.inefficient_ordering
            ):
                self.ordering.append(order_string)

            else:
                self.add_inefficient_order_by(order_string)

    @safe_call
    def add_inefficient_order_by(self, ordering):
        if ordering.startswith('-'):
            field_name = ordering[1:]
            reversed = True
        else:
            field_name = ordering
            reversed = False

        self.inefficient_ordering.append((field_name, reversed))

    def init_predicate(self, parent_predicate, node):
        if isinstance(node, WhereNode):
            if node.connector == OR:
                compound_op = COMPOUND_OP_OR
            elif node.connector == AND:
                compound_op = COMPOUND_OP_AND
            else:
                raise InvalidQueryOpException(node.connector)

            predicate = CompoundPredicate(compound_op, node.negated)

            for child in node.children:
                self.init_predicate(
                    predicate,
                    child
                )

            if parent_predicate:
                parent_predicate.add_child(predicate)
        else:
            try:
                decoded_child = self._decode_child(node)
                assert parent_predicate

                parent_predicate.add_filter(*decoded_child)
                self.filters.append(decoded_child)

            except EmptyResultSet:
                pass

            predicate = None

        return predicate

    # FIXME: This is bad. We're modifying the WhereNode object that's passed in to us
    # from the Django ORM. We should do the pruning as we build our predicates, not
    # munge the WhereNode.
    def remove_unnecessary_nodes(self, node, retain_root_node):
        if isinstance(node, WhereNode):
            child_count = len(node.children)
            for i in range(child_count):
                node.children[i] = self.remove_unnecessary_nodes(
                    node.children[i],
                    False
                )
            if (
                (not retain_root_node) and
                (not node.negated) and
                (len(node.children) == 1)
            ):
                node = node.children[0]

        return node

    @safe_call
    def add_filters(self, filters):
        """
        Traverses the given Where tree and adds the filters to this query
        """

        assert isinstance(filters, WhereNode)
        self.remove_unnecessary_nodes(filters, True)
        self.root_predicate = self.init_predicate(None, filters)


class SQLCompiler(NonrelCompiler):
    query_class = CassandraQuery

    def execute_sql(
        self,
        result_type=MULTI
    ):
        return super(SQLCompiler, self).execute_sql(result_type)


class SQLInsertCompiler(
    NonrelInsertCompiler,
    SQLCompiler
):
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
                if 'pk__token' in row:
                    del row['pk__token']

                if meta.has_auto_field and meta.pk.column not in row.keys():
                    if meta.pk.get_internal_type() == 'AutoField':
                        '''
                        Using the default integer based AutoField
                        is inefficient due to the fact that Cassandra
                        has to count all the rows to determine the
                        correct integer id.
                        '''
                        row[meta.pk.column] = (
                            column_family.objects.all().count() + 1
                        )

                    elif hasattr(meta.pk, 'get_auto_value'):
                        row[meta.pk.column] = meta.pk.get_auto_value()

                    else:
                        raise Exception(
                            'Please define a "get_auto_value" method '
                            'on your custom AutoField that returns the '
                            'next appropriate value for automatic primary '
                            'key for your database model.'
                        )

                inserted = column_family.batch(b).create(
                    **row
                )

                inserted_row_keys.append(inserted.pk)

        if return_id:
            if len(inserted_row_keys) == 1:
                return inserted_row_keys[0]

            else:
                return inserted_row_keys


class SQLUpdateCompiler(
    NonrelUpdateCompiler,
    SQLCompiler
):
    def update(
        self,
        values
    ):
        value_dict = {}
        fields = []
        for value in values:
            field = value[0]
            if 'Token' == field.get_internal_type():
                continue

            fields.append(field)
            value_dict[
                field.db_column
                if field.db_column
                else field.column
            ] = value[1]

        query = CassandraQuery(
            self,
            fields,
            allows_inefficient=False
        )
        query.add_filters(self.query.where)
        range_predicates = []
        if query.root_predicate.children:
            for predicate in query.root_predicate.children:
                range_predicates.append(predicate)
        query.get_row_range(range_predicates).update(**value_dict)
        return True


class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass
