from django.conf import settings
from django.db.models import (
    Model,
    CharField
)
from djangocassandra.db.backends.cassandra.base import DatabaseWrapper


def connect_db():
    connection = DatabaseWrapper(settings.DATABASES['default'])
    connection.get_session()
    return connection


def create_db(connection):
    class TestModel(Model):
        column_1 = CharField(max_length=16)
        column_2 = CharField(max_length=16)

    connection.creation.sql_create_model(
        TestModel,
        None
    )


def populate_db(connection):
    pass


def destroy_db(connection):
    session = connection.get_session()
    for keyspace in settings.DATABASES['default']['KEYSPACES']:
        session.execute('drop keyspace %s;' % keyspace,)
