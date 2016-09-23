import sys
import random
random.seed()

import string

from django.conf import settings

from cassandra.cqlengine.management import drop_keyspace

from djangocassandra.db.backends.cassandra.base import DatabaseWrapper


def connect_db():
    connection = DatabaseWrapper(settings.DATABASES['default'])
    connection_params = connection.get_connection_params()
    connection.get_new_connection(connection_params)
    return connection


def create_model(connection, model):
    connection.creation.sql_create_model(
        model,
        None
    )


def populate_db(connection, values):
    for value in values:
        value.save()


def destroy_db(connection):
    if None is not connection:
        keyspace_names = [
            key for key in settings.DATABASES['default']['KEYSPACES'].keys()
        ]
        for keyspace in keyspace_names:
            drop_keyspace(keyspace)


def random_float(minimum=sys.float_info.min, maximum=sys.float_info.max):
    return random.uniform(minimum, maximum)


def random_integer(minimum=0, maximum=sys.maxint):
    return random.randint(minimum, maximum)


def random_string(length=8):
    return ''.join(
        random.choice(string.ascii_letters) for x in xrange(length)
    )
