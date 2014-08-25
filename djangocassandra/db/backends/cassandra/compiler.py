from uuid import uuid4

from django.db.utils import DatabaseError

from django.db.models.sql.constants import MULTI
from django.db.models.sql.where import (
    WhereNode,
    Constraint,
    EverythingNode,
    AND
)

from djangotoolbox.db.basecompiler import (
    NonrelQuery,
    NonrelCompiler,
    NonrelInsertCompiler,
    NonrelUpdateCompiler,
    NonrelDeleteCompiler
)

from cassandra.query import SimpleStatement

from .utils import (
    sort_rows,
    filter_rows
)


class CassandraQuery(NonrelQuery):
    MAX_RESULT_COUNT = 10000
    where_class = WhereNode

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
        self.pk_column = self.meta.pk.column
        self.column_family = self.meta.db_table
        self.columns = fields
        self.where = None
        self.ordering = None
        self.cache = None
        self.allows_inefficient = True  # TODO: Make this a config setting
        self.can_filter_efficiently = True
        self.can_order_efficiently = True

        if hasattr(self.query.model, 'Cassandra'):
            self.column_family_settings = self.query.model.Cassandra.__dict__

        else:
            self.column_family_settings = (
                self.connection.creation.default_cassandra_model_settings
            )

        self.partition_key_columns = self.column_family_settings.get(
            'partition_key_columns',
            [self.pk_column]
        )
        self.clustering_key_columns = self.column_family_settings.get(
            'clustering_key_columns',
            []
        )

        self.indexed_columns = []
        self.field_name_to_column_name = {}
        for field in fields:
            if field.db_column:
                column = field.db_column

            else:
                column = field.column

            if field.db_index:
                self.indexed_columns.append(column)

            self.field_name_to_column_name[field.name] = column

        self.partition_key = self.query.model

    def _build_ordering_clause(self):
        if self.ordering and self.can_order_efficiently:
            ordering = self.ordering[0]
            ordering_clause = ' '.join([
                'ORDER BY',
                ordering[0],
                'ASC' if ordering[1] else 'DESC'
            ])

        else:
            ordering_clause = ''

        return ordering_clause

    def _build_paging_clause(
        self,
        low_mark=None,
        high_mark=None
    ):
        paging_clause = []
        if low_mark:
            paging_clause.append(' '.join([
                'TOKEN(%s)' % (self.pk_column,),
                '>=',
                'TOKEN(%s)' % (low_mark,)
            ]))

        return ' AND '.join(paging_clause)

    def _build_columns_clause(self):
        if self.fields:
            columns = ', '.join([
                self.field_name_to_column_name.get(
                    field.name,
                    ''
                ) for field in self.fields
            ])

        else:
            columns = '*'

        return columns

    def _build_where_clause(
        self,
        paging_clause=None
    ):
        if not self.can_filter_efficiently:
            return ''

        where_statement, where_params = self.where.as_sql(
            self.connection.ops.quote_name,
            self.connection
        )
        if where_statement and where_params:
            # TODO: WhereNode should probably be subclassed
            #       to appropriately format the where clause
            #       although this will probably only ever be
            #       used internally so not super critical.
            where_statement = where_statement.replace(
                '(',
                ''
            ).replace(
                ')',
                ''
            )

            quoted_params = []
            for param in where_params:
                # TODO: Where params should also be sanitized.
                if isinstance(param, basestring):
                    param = "'" + param + "'"

                quoted_params.append(param)
            where_clause = where_statement % tuple(quoted_params)

        else:
            where_clause = ''

        if paging_clause:
            if where_clause:
                where_clause = ' AND '.join([
                    where_clause,
                    paging_clause
                ])

            else:
                where_clause = paging_clause

        if where_clause:
            where_clause = ' '.join([
                'WHERE',
                where_clause
            ])

        return where_clause

    def _get_query_results(
        self,
        low_mark=None,
        high_mark=None
    ):
        if None is not high_mark and high_mark <= low_mark:
            return None

        if None is self.cache:
            if None is self.where:
                self.where = EverythingNode()

            # TODO: !!!!vvv NEED TO SANITIZE ALL OF THIS vvv!!!!

            ordering_clause = self._build_ordering_clause()

            if high_mark:
                limit_clause = 'LIMIT %s' % (high_mark,)

            else:
                limit_clause = 'LIMIT %s' % (10000,)

            columns_clause = self._build_columns_clause()

            column_family = self.column_family

            paging_clause = self._build_paging_clause(
                low_mark,
                high_mark
            )

            where_clause = self._build_where_clause(paging_clause)

            # TODO: !!!!^^^ NEED TO SANITIZE ALL OF THIS ^^^!!!!

            select_statement = ' '.join([
                'SELECT',
                columns_clause,
                'FROM',
                column_family,
                where_clause,
                ordering_clause,
                limit_clause
            ])

            try:
                session = self.connection.get_session(
                    keyspace=self.compiler.using
                )
                self.cache = session.execute(select_statement)

            except Exception:
                raise

            if not self.can_filter_efficiently:
                self.cache = filter_rows(self.cache, self.where)

            if not self.can_order_efficiently and self.ordering:
                sort_rows(self.cache, self.ordering)

        return self.cache

    def fetch(
        self,
        low_mark=None,
        high_mark=None
    ):
        results = self._get_query_results(
            low_mark,
            high_mark
        )

        for result in results:
            yield result

    def count(
        self,
        limit=None
    ):
        # TODO: Actually I guess CQL does support statements like:
        #
        #           SELECT COUNT(*) FROM column_family
        #
        #       Still don't understand if this uses counter column families
        #       or the efficiency implications of this.
        raise DatabaseError(
            'Cassandra does not support counting rows. '
            'Create a counter column family and query that instead.'
        )

    def delete(
        self,
        columns=set()
    ):
        results = self._get_query_results()
        column_family = self.column_family
        columns_clause = self._build_columns_clause()
        keys = ', '.join([
            result.pk for result in results
        ])

        cql = ' '.join([
            'DELETE',
            columns_clause,
            'FROM',
            column_family,
            'WHERE KEY IN (',
            keys,
            ')'
        ])

        # TODO: Figure out how to handle configurable consistancy levels.
        #       Configurable consistancy level will most likely live on the
        #       Compiler (self.compiler.consistancy_level or whatever).
        delete_statement = SimpleStatement(cql)

        session = self.connection.get_session(keyspace=self.compiler.using)
        session.execute(delete_statement)

    def order_by(
        self,
        ordering
    ):
        if isinstance(ordering, bool):
            return

        if len(ordering) > 1:
            if self.allows_inefficient:
                self.can_order_efficiently = False
                # TODO: Need to raise a warning whenever
                #       we are allowing an inefficient
                #       query.

            else:
                raise DatabaseError(
                    'ORDER BY clauses can select a single column '
                    'only. That column has to be the second column '
                    'in a compound PRIMARY KEY.'
                )

        self.ordering = []
        order = ordering[0]

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
                field_name = field.column

            else:
                raise DatabaseError(
                    'Invalid ordering specification: %s' % order,
                )

            column_name = self.field_name_to_column_name.get(
                field_name,
                field_name
            )

            if self.can_order_efficiently:
                if (
                    not self.clustering_key_columns or
                    column_name != self.clustering_key_columns[0]
                ):
                    if self.allows_inefficient:
                        self.can_order_efficiently = False
                        # TODO: Warning about efficiency

                    else:
                        raise DatabaseError(
                            'ORDER BY clauses can select a single column '
                            'only. That column has to be the second column '
                            'in a compound PRIMARY KEY.'
                        )

            self.ordering.append((column_name, ascending))

    def add_filter(
        self,
        field,
        lookup_type,
        negated,
        value
    ):
        if None is self.where:
            self.where = self.where_class()

        if field.column not in self.indexed_columns:
            if self.allows_inefficient:
                self.can_filter_efficiently = False
                # TODO: Raise Efficiency Warning

            else:
                raise DatabaseError(
                    'Filtering on columns that are not part of the '
                    'partition/clustering key or that do not have '
                    'secondary indexes is not supported by Cassandra. '
                    'Either add a secondary index or reevaluate your '
                    'database schema.'
                )

        constraint = (
            Constraint(
                None,
                field.column,
                field
            ),
            lookup_type,
            value
        )

        clause = self.where_class()
        clause.add(constraint, AND)
        self.where.add(clause, AND)


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
        if hasattr(self.query.model, 'Cassandra'):
            cassandra_cf_opts = self.query.model.Cassandra
            pk_columns = [
                cassandra_cf_opts.partition_key_columns +
                cassandra_cf_opts.clustering_key_columns
            ]
        else:
            pk_columns = [meta.pk.column]

        column_family = meta.db_table

        inserted_row_keys = []
        for row in values:
            primary_key = []
            column_list = []
            value_list = []
            for column, value in row.items():
                if column in pk_columns:
                    primary_key.append(value)
                column_list.append(column)
                value_list.append(value)

            if not primary_key:
                primary_key.append(uuid4())
                len_primary_key = len(primary_key)
                assert(len_primary_key == len(pk_columns))
                assert(len_primary_key == 1)
                column_list.extend(pk_columns)
                value_list.extend(primary_key)

            columns_clause = ', '.join(column_list)
            values_placeholder = ", ".join([
                '%s' for _ in value_list
            ])

            cql_statement = ''.join([
                'INSERT INTO ',
                column_family,
                ' ( ',
                columns_clause,
                " ) VALUES ( ",
                values_placeholder,
                " )"
            ])

            session = self.connection.get_session(keyspace=self.using)
            try:
                session.execute(cql_statement, value_list)

            except:
                raise

            inserted_row_keys.append(':'.join([
                part if isinstance(part, basestring) else
                str(part) for part in primary_key
            ]))

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
