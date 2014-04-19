from django.db.utils import DatabaseError

from djangotoolbox.db.creation import NonrelDatabaseCreation

from cassandra.metadata import (
    TableMetadata,
    ColumnMetadata,
    IndexMetadata
)


class DatabaseCreation(NonrelDatabaseCreation):
    def sql_create_model(
        self,
        model,
        style,
        known_models=set()
    ):
        connection_settings = self.connection.settings_dict

        meta = model._meta

        if hasattr(model, 'Cassandra'):
            cassandra_settings = model.Cassandra

        else:
            cassandra_settings = self.default_cassandra_model_settings

        if hasattr(cassandra_settings, 'keyspace'):
            keyspace = cassandra_settings.keyspace

        else:
            keyspace = connection_settings.get('DEFAULT_KEYSPACE')

        keyspace_metadata = self.connection._cluster.metadata.keyspaces.get(
            keyspace
        )
        if not keyspace_metadata:
            keyspace_metadata = self.connection._create_keyspace(
                keyspace
            )

        table_options = (
            self.connection.default_table_options.copy()
        )
        if hasattr(cassandra_settings, 'table_options'):
            if not isinstance(cassandra_settings.table_optoins, dict):
                raise DatabaseError(
                    'The value of table_optoins in the Cassandra class '
                    'must be a dict containing overrides for the default'
                    'column family options.'
                )
            table_options.update(cassandra_settings.table_metadata)

        table_metadata = TableMetadata(
            keyspace_metadata,
            meta.db_table,
            options=table_options
        )

        partition_key = []
        clustering_key = []
        columns = {}
        for field in meta.local_fields:
            column_name = field.db_column if field.db_column else field.column
            column = ColumnMetadata(
                table_metadata,
                column_name
            )
            if field.db_index:
                index = IndexMetadata(
                    column,
                    index_name='_'.join([
                        'idx',
                        meta.db_table,
                        column_name
                    ])
                )
                column.index = index

            if column_name in cassandra_settings.partition_key_columns:
                partition_key.append(column)

            if column_name in cassandra_settings.clustering_key_columns:
                clustering_key.append(column)

        if columns:
            table_metadata.columns = columns

        if partition_key:
            table_metadata.partition_key = partition_key
        if clustering_key:
            table_metadata.clustering_key = clustering_key

        session = self.connection.get_session(keyspace=keyspace)
        session.execute(table_metadata.as_cql_query())

        return [], {}
