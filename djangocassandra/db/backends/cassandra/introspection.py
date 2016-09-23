import itertools

from django.db.backends import BaseDatabaseIntrospection
from djangotoolbox.db.base import NonrelDatabaseIntrospection


class DatabaseIntrospection(NonrelDatabaseIntrospection):
    def get_table_list(
        self,
        cursor=None
    ):
        if None is cursor:
            cursor = self.connection.cursor()

        current_keyspace = cursor.keyspace

        keyspaces = [
            key for key in self.connection.settings_dict.get(
                'KEYSPACES'
            ).keys()
        ]

        table_list = []
        '''
        These are where the schema information for the cluster is
        stored for Cassandra version 2.x
        '''
        schema_keyspace = 'system'
        schema_table_name = 'schema_columnfamilies'
        schema_table_name_field = 'columnfamily_name'

        if 'system_schema' in self.connection.cluster.metadata.keyspaces:
            '''
            If there is a 'system_schema' keyspace then we are on
            Cassandra 3.x and need to look for the schema info here.
            '''
            schema_keyspace = 'system_schema'
            schema_table_name = 'tables'
            schema_table_name_field = 'table_name'

        try:
            cursor.set_keyspace(schema_keyspace)
            for keyspace in keyspaces:
                table_list = itertools.chain(
                    table_list, cursor.execute(
                        ''.join([
                            'SELECT ',
                            schema_table_name_field,
                            ' from ',
                            schema_table_name,
                            ' where keyspace_name=\'',
                            keyspace,  # TODO: SANITIZE ME JUST IN CASE!!!
                            '\''
                        ])
                    )
                )

        finally:
            if None is not current_keyspace:
                cursor.set_keyspace(current_keyspace)

        return [row[schema_table_name_field] for row in table_list]

    def table_names(self, cursor=None):
        return BaseDatabaseIntrospection.table_names(self, cursor)
