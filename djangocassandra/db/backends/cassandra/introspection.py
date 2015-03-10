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
        try:
            cursor.set_keyspace('system')
            for keyspace in keyspaces:
                table_list = itertools.chain(
                    table_list, cursor.execute(
                        ''.join([
                            'SELECT columnfamily_name from ',
                            'schema_columnfamilies where keyspace_name=\'',
                            keyspace,  # TODO: SANITIZE ME JUST IN CASE!!!
                            '\''
                        ])
                    )
                )

        finally:
            if None is not current_keyspace:
                cursor.set_keyspace(current_keyspace)

        return [row['columnfamily_name'] for row in table_list]

    def table_names(self, cursor):
        return BaseDatabaseIntrospection.table_names(self, cursor)
