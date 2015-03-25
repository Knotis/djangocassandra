import os
import uuid

from cassandra.metadata import (
    SimpleStrategy
)

SECRET_KEY = uuid.uuid4().hex

DATABASES = {
    'default': {
        'ENGINE': 'djangocassandra.db.backends.cassandra',
        'DEFAULT_KEYSPACE': 'test',
        'CONTACT_POINTS': (os.environ.get(
            'DJANGOCASSANDRA_TEST_HOST',
            'localhost'
        ),),
        'PORT': int(os.environ.get(
            'DJANGOCASSANDRA_TEST_PORT',
            9042
        )),
        'KEYSPACES': {
            'test': {
                'replication_factor': 1,
                'strategy_class': SimpleStrategy.name
            }
        }
    }
}
