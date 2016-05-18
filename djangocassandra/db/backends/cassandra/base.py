from uuid import UUID

from djangotoolbox.db.base import (
    NonrelDatabaseFeatures,
    NonrelDatabaseOperations,
    NonrelDatabaseWrapper,
    NonrelDatabaseClient,
    NonrelDatabaseValidation
)

from cassandra import (
    ConsistencyLevel
)
from cassandra.metadata import (
    SimpleStrategy
)

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace

from .creation import DatabaseCreation
from .introspection import DatabaseIntrospection
from .schema import CassandraSchemaEditor
from .cursor import CassandraCursor


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
        sequence_list,
        allow_cascade=False
    ):
        if tables:
            cql = [
                'use %s;' % (
                    style.SQL_FIELD(self.connection.keyspace),
                )
            ]
            for table in tables:
                cql.append('%s %s;' % (
                    style.SQL_KEYWORD('TRUNCATE'),
                    style.SQL_FIELD(self.quote_name(table))
                ))

            return cql

        else:
            return []

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
    default_settings = {
        'ENGINE': 'djangocassandra.db.backends.cassandra',
        'DEFAULT_KEYSPACE': 'test',
        'CONTACT_POINTS': ('localhost',),
        'PORT': 9042,
        'KEYSPACES': {
            'test': {
                'replication_factor': 1,
                'strategy_class': SimpleStrategy.name
            }
        }
    }

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
        self.session = None
        self.cluster = None

    def schema_editor(self):
        return CassandraSchemaEditor(self)

    def create_cursor(self):
        self.ensure_connection()
        return CassandraCursor(connection.get_session())

    def _cursor(self):
        return self.create_cursor()

    def ensure_connection(self):
        super(DatabaseWrapper, self).ensure_connection()

    def connect(self):
        super(DatabaseWrapper, self).connect()

    def close(self):
        super(DatabaseWrapper, self).close()

    def get_connection_params(self):
        '''
        Gets cluster settings from Django settings module using keys/values
        the same as in the datastax documentation See:

            http://datastax.github.io/python-driver/api/cassandra/cluster.html

        for descriptions of what each setting expects.
        '''
        keyspace = self.settings_dict.get(
            'DEFAULT_KEYSPACE',
            'django'
        )

        settings = self.settings_dict
        defaults = {
            'CONTACT_POINTS': ('localhost',),
            'PORT': 9042
        }

        contact_points = settings.get(
            'CONTACT_POINTS',
            defaults['CONTACT_POINTS']
        )

        port = self.settings_dict.get(
            'PORT',
            defaults['PORT']
        )

        port = port if port else defaults['PORT']

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

        return {
            'lazy_connect': True,
            'retry_connect': True,
            'contact_points': contact_points,
            'keyspace': keyspace,
            'consistency': ConsistencyLevel.ONE,
            'lazy_connect': True,
            'retry_connect': True,
            'port': port,
            'compression': compression,
            'auth_provider': auth_provider,
            'load_balancing_policy': load_balancing_policy,
            'reconnection_policy': reconnection_policy,
            'default_retry_policy': default_retry_policy,
            'conviction_policy_factory': conviction_policy_factory,
            'metrics_enabled': metrics_enabled,
            'connection_class': connection_class,
            'ssl_options': ssl_options,
            'sockopts': sockopts,
            'cql_version': cql_version,
            'executor_threads': executor_threads,
            'max_schema_agreement_wait': max_schema_agreement_wait,
            'control_connection_timeout': control_connection_timeout
        }

    def get_new_connection(self, connection_settings):
        contact_points = connection_settings.pop(
            'contact_points',
            self.default_settings['CONTACT_POINTS']
        )
        keyspace = connection_settings.pop(
            'keyspace',
            self.settings_dict['DEFAULT_KEYSPACE']
        )

        self.keyspace = keyspace
        self.session = connection.get_session()
        if not(self.session is None or self.session.is_shutdown):
            return CassandraCursor(self.session)

        connection.setup(
            contact_points,
            keyspace,
            **connection_settings
        )

        self.cluster = connection.get_cluster()
        self.session = connection.get_session()
        self.session.default_timeout = None  # Should be in config.

        return CassandraCursor(self.session)

    def current_keyspace(self):
        if not self.keyspace:
            self.keyspace = self.settings_dict.get('DEFAULT_KEYSPACE')

        if None is self.keyspace:
            self.keyspace = 'django'

        return self.keyspace

    def create_keyspace(
        self,
    ):
        settings = self.settings_dict

        keyspace_default_settings = {
            'durable_writes': True,
            'strategy_class': SimpleStrategy.name,
            'replication_factor': 3
        }

        keyspace_settings = settings.get(
            'KEYSPACES', {}
        ).get(
            self.keyspace, {}
        )

        keyspace_default_settings.update(keyspace_settings)
        keyspace_settings = keyspace_default_settings

        self.ensure_connection()
        create_keyspace(
            self.keyspace,
            **keyspace_settings
        )
