from django.conf import settings
from django.db.models import (
    Model,
    CharField
)
from djangocassandra.db.backends.cassandra.base import DatabaseWrapper
from djangocassandra.db.backends.cassandra.compiler import SQLInsertCompiler


def connect_db():
    connection = DatabaseWrapper(settings.DATABASES['default'])
    connection.get_session()
    return connection


def create_db(connection, model):
    connection.creation.sql_create_model(
        model,
        None
    )


def populate_db(connection, values):
    for value in values:
        value.save()    


def destroy_db(connection):
    session = connection.get_session()
    for keyspace in settings.DATABASES['default']['KEYSPACES']:
        session.execute('drop keyspace %s;' % keyspace,)
