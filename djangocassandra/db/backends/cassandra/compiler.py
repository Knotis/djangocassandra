from django.db.utils import DatabaseError
from django.db.models.sql.where import (
    WhereNode,
    EverythingNode,
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

from cassandra.query import SimpleStatement


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
        self.pk_column = self.meta.pk.column
        self.column_family = self.meta.db_table
        self.columns = fields
        self.where = None
        self.ordering = None
        self.cache = None

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

    def _build_ordering_clause(self):
        if self.ordering:
            ordering = self.ordering[0]
            ordering_clause = ' '.join([
                'ORDER BY',
                ordering[0],
                'AESC' if ordering[1] else 'DESC'
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

        if high_mark:
            paging_clause.append(' '.join([
                'TOKEN(%s)' % (self.pk_column,),
                '<=',
                'TOKEN(%s)' % (high_mark,)
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

            paging_clause = self._build_paging_clause(
                low_mark,
                high_mark
            )

            columns_clause = self._build_columns_clause()

            column_family = self.column_family
            # TODO: !!!!^^^ NEED TO SANITIZE ALL OF THIS ^^^!!!!

            where_statement, where_params = self.where.as_sql()
            if where_statement and where_params:
                where_clause = where_statement % (where_params,)

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

            select_statement = ['SELECT']

            select_statement = ' '.join([
                'SELECT',
                columns_clause,
                'FROM',
                column_family,
                where_clause,
                ordering_clause
            ])

            try:
                session = self.connection.get_session(
                    keyspace=self.compiler.using
                )
                self.cache = session.execute(select_statement)

            except Exception:
                raise

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
            raise DatabaseError(
                'ORDER BY clauses can select a single column only. '
                'That column has to be the second column in a compound '
                'PRIMARY KEY.'
            )

        self.ordering = []
        order = ordering[0]

        if 1 == len(order):
            if order.startswith('-'):
                field_name = order[1:]
                ascending = False

            else:
                field_name = order
                ascending = True

        elif 2 == len(order):
            field_name, ascending = order

        else:
            raise DatabaseError(
                'Invalid ordering specification: %s' % order,
            )

        column_name = self.field_name_to_column_name.get(
            field_name,
            field_name
        )

        self.ordering = [(column_name, ascending)]

    def add_filter(
        self,
        field,
        lookup_type,
        negated,
        value
    ):
        pass


class SQLCompiler(NonrelCompiler):
    query_class = CassandraQuery
