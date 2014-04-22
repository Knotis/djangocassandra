from django.db.backends import BaseDatabaseIntrospection
from djangotoolbox.db.base import NonrelDatabaseIntrospection


class DatabaseIntrospection(NonrelDatabaseIntrospection):
    def get_table_list(
        self,
        cursor=None
    ):
        session = self.connection.get_session()
        current_keyspace = session.keyspace
        session.set_keyspace('system')
        table_list = session.execute(''.join([
            'SELECT columnfamily_name from ',
            'schema_columnfamilies where keyspace_name=\'',
            current_keyspace,  # TODO: SANITIZE ME JUST IN CASE!!!
            '\''
        ]))
        session.set_keyspace(current_keyspace)
        return [row[0] for row in table_list]

    def table_names(self):
        return BaseDatabaseIntrospection.table_names(self)
