from uuid import UUID

from djangotoolbox.db.base import (
    NonrelDatabaseFeatures,
    NonrelDatabaseOperations,
    NonrelDatabaseWrapper,
    NonrelDatabaseClient,
    NonrelDatabaseValidation
)

from cassandra import InvalidRequest
from cassandra.cluster import (
    Cluster,
    dict_factory
)
from cassandra.metadata import (
    KeyspaceMetadata,
    SimpleStrategy
)

from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection


class DatabaseFeatures(NonrelDatabaseFeatures):
    string_based_auto_field = True

    def __init__(self, connection):
        super(
            DatabaseFeatures,
            self
        ).__init__(connection)

        self.supports_deleting_related_objects = connection.settings_dict.get(
            'CASSANDRA_ENABLE_CASCADING_DELETES',
            False
        )


class DatabaseOperations(NonrelDatabaseOperations):
    compiler_module = __name__.rsplit('.', 1)[0] + '.compiler'

    def sql_flush(
        self,
        style,
        tables,
        sequence_list
    ):
        for table_name in tables:
            self.connection.creation.flush_table(table_name)

        return ''

    def _value_for_db(
        self,
        value,
        field,
        field_kind,
        db_type,
        lookup
    ):
        return super(DatabaseOperations, self)._value_for_db(
            value,
            field,
            field_kind,
            db_type,
            lookup
        )

    def _value_from_db(
        self,
        value,
        field,
        field_kind,
        db_type
    ):
        return super(DatabaseOperations, self)._value_from_db(
            value,
            field,
            field_kind,
            db_type
        )

    def value_to_db_auto(
        self,
        value
    ):
        if isinstance(value, basestring):
            value = UUID(value)

        return value


class DatabaseClient(NonrelDatabaseClient):
    pass


class DatabaseValidation(NonrelDatabaseValidation):
    pass


class DatabaseWrapper(NonrelDatabaseWrapper):
    def __init__(
        self,
        *args,
        **kwargs
    ):
        super(
            DatabaseWrapper,
            self
        ).__init__(
            *args,
            **kwargs
        )

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.validation = DatabaseValidation(self)
        self.introspection = DatabaseIntrospection(self)
        self._cluster = self._configure_cluster()

        '''
        Do we want to support multiple sessions?
        I'm having trouble coming up with a use case.
        If so it shouldn't be to hard to change this
        so that multiple sessions are stored in an
        list or a dict.
        '''
        self._session = None

    def _configure_cluster(self):
        '''
        Returns a new Cluster instance initialized with
        values from settings.py. See:

            http://datastax.github.io/python-driver/api/cassandra/cluster.html

        for descriptions of what each setting expects.
        '''
        settings = self.settings_dict

        contact_points = settings.get(
            'CONTACT_POINTS',
            ('localhost',)
        )

        port = self.settings_dict.get(
            'PORT',
            9042
        )

        compression = settings.get(
            'COMPRESSION',
            True
        )

        auth_provider = settings.get(
            'AUTH_PROVIDER'
        )

        load_balancing_policy = settings.get(
            'LOAD_BALANCING_POLICY'
        )

        reconnection_policy = settings.get(
            'RECONNECTION_POLICY'
        )

        default_retry_policy = settings.get(
            'DEFAULT_RETRY_POLICY'
        )

        conviction_policy_factory = settings.get(
            'CONVICTION_POLICY_FACTORY'
        )

        metrics_enabled = settings.get(
            'METRICS_ENABLED',
            False
        )

        connection_class = settings.get(
            'CONNECTION_CLASS'
        )

        ssl_options = settings.get(
            'SSL_OPTIONS'
        )

        sockopts = settings.get(
            'SOCKOPTS'
        )

        cql_version = settings.get(
            'CQL_VERSION'
        )

        executor_threads = settings.get(
            'EXECUTOR_THREADS',
            2
        )

        max_schema_agreement_wait = settings.get(
            'MAX_SCHEMA_AGREEMENT_WAIT',
            10
        )

        control_connection_timeout = settings.get(
            'CONTROL_CONNECTION_TIMEOUT',
            2.0
        )

        return Cluster(
            contact_points=contact_points,
            port=port,
            compression=compression,
            auth_provider=auth_provider,
            load_balancing_policy=load_balancing_policy,
            reconnection_policy=reconnection_policy,
            default_retry_policy=default_retry_policy,
            conviction_policy_factory=conviction_policy_factory,
            metrics_enabled=metrics_enabled,
            connection_class=connection_class,
            ssl_options=ssl_options,
            sockopts=sockopts,
            cql_version=cql_version,
            executor_threads=executor_threads,
            max_schema_agreement_wait=max_schema_agreement_wait,
            control_connection_timeout=control_connection_timeout
        )

    def _create_keyspace(
        self,
        keyspace,
        session=None
    ):
        if not session:
            session = self.get_session(keyspace)

        if keyspace in self._cluster.metadata.keyspaces:
            ''' This keyspace already exists, nothing to do '''
            return self._cluster.metadata.keyspaces[keyspace]

        settings = self.settings_dict

        keyspace_default_settings = {
            'DURABLE_WRITES': True,
            'REPLICATION_STRATEGY_CLASS': SimpleStrategy.name,
            'REPLICATION_STRATEGY_OPTIONS': {
                'replication_factor': 3
            }
        }

        keyspace_settings = settings.get(
            'KEYSPACES', {}
        ).get(
            keyspace, {}
        )

        keyspace_default_settings.update(keyspace_settings)
        keyspace_settings = keyspace_default_settings
        keyspace_metadata = KeyspaceMetadata(
            keyspace,
            keyspace_settings['DURABLE_WRITES'],
            keyspace_settings['REPLICATION_STRATEGY_CLASS'],
            keyspace_settings['REPLICATION_STRATEGY_OPTIONS']
        )

        self._cluster.metadata.keyspaces[keyspace] = keyspace_metadata

        session.execute(keyspace_metadata.as_cql_query())

        return keyspace_metadata

    def _open_session(
        self,
        keyspace=None
    ):
        if not keyspace or keyspace == 'default':
            keyspace = self.settings_dict.get('DEFAULT_KEYSPACE')

        if None is keyspace:
            keyspace = 'django'

        if not self._session or self._session.is_shutdown:
            self._session = self._cluster.connect()
            self._session.row_factory = dict_factory

        try:
            self._session.set_keyspace(keyspace)

        except InvalidRequest:
            '''
            Try to configure and create the keyspace if we get an exception.
            '''
            self._create_keyspace(
                keyspace,
                session=self._session
            )
            self._session.set_keyspace(keyspace)

        return self._session

    def get_session(
        self,
        keyspace=None
    ):
        if not self._cluster:
            self._cluster = self._configure_cluster()

        return self._open_session(
            keyspace=keyspace
        )
