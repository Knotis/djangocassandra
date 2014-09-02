from unittest import TestCase


class TestImport(TestCase):
    def test_1(self):
        from djangocassandra.db.backends.cassandra.base import DatabaseClient


class TestFiltering(TestCase):
    def test_filter(self):
        from djangocassandra.db.backends.cassandra.compiler import (
            CassandraQuery,
            SQLCompiler
        )
        from django.db.models import Model
        from django.db.models.fields import CharField
        class TestModel(Model):
            foo = CharField()

        query = CassandraQuery(SQLCompiler, [])
        query.add_filter(
            TestModel.foo,
            None,
            False,
            'a'
        )
