import itertools

from django.db.backends import BaseDatabaseIntrospection
from djangotoolbox.db.base import NonrelDatabaseIntrospection
from cqlengine.connection import get_session


class DatabaseIntrospection(NonrelDatabaseIntrospection):
    def get_table_list(
        self,
        cursor=None
    ):
        session = get_session()
        current_keyspace = session.keyspace

        keyspaces = [
            key for key in self.connection.settings_dict.get('KEYSPACES').keys()
        ]
        
        table_list = []
        session.set_keyspace('system')
        for keyspace in keyspaces:
            table_list = itertools.chain(
                table_list, session.execute(
                    ''.join([
                        'SELECT columnfamily_name from ',
                        'schema_columnfamilies where keyspace_name=\'',
                        keyspace,  # TODO: SANITIZE ME JUST IN CASE!!!
                        '\''
                    ])
                )
            )

        if None is not current_keyspace:
            session.set_keyspace(current_keyspace)

        return [row['columnfamily_name'] for row in table_list]

    def table_names(self):
        return BaseDatabaseIntrospection.table_names(self)
