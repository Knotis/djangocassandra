from uuid import uuid4

from django.db.utils import DatabaseError

from django.db.models.sql.constants import MULTI
from djangotoolbox.db.basecompiler import (
    NonrelQuery,
    NonrelCompiler,
    NonrelInsertCompiler,
    NonrelUpdateCompiler,
    NonrelDeleteCompiler
)

from cqlengine.query import BatchQuery

from djangocassandra.db.models import (
    get_column_family
)


class InefficientQueryWarning(RuntimeWarning):
    pass


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
        self.allows_inefficient = True  # TODO: Make this a config setting
        self.can_filter_efficiently = True
        self.can_order_efficiently = True

        self.column_family_class = get_column_family(
            self.connection,
            self.query.model
        )
        self.cql_query = self.column_family_class.objects.all().values_list(
            *[field.column for field in self.meta.fields]
        )

    def _get_query_results(
        self,
        low_mark=None,
        high_mark=None
    ):
        if None is not high_mark and high_mark <= low_mark:
            return None

        results = []
        for result in self.cql_query:
            result_dict = {}

            for i in xrange(len(self.meta.fields)):
                result_dict[self.meta.fields[i].name] = result[i]

            results.append(result_dict)

        return results

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
            return

        self.ordering = []

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
                raise DatabaseError(
                    'Invalid ordering specification: %s' % order,
                )

            column_name = self.field_name_to_column_name.get(
                field_name,
                field_name
            )

            order_string = ''.join([
                '' if accending else '-',
                field_name
            ])

            self.cql_query.order_by(order_string)

    def add_filter(
        self,
        field,
        lookup_type,
        negated,
        value
    ):
        if negated:
            raise Exception('Cassandra does not support negated queries')

        supported_lookup_types = [
            'in',
            'gt',
            'gte',
            'lt',
            'lte',
        ]

        if lookup_type not in supported_lookup_types:
            raise Exception(
                'The lookup type "{}" is not supported'.format(lookup_type)
            )

        filter_params = {}
        filter_params['__'.join([
            field,
            lookup_type
        ])] = value

        self.cql_query.filter(**filter_params)


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
