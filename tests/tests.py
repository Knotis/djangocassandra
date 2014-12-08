from unittest import TestCase

from django.db.models import (
    Model,
    CharField
)

from util import (
    connect_db,
    create_db,
    populate_db,
    destroy_db
)

class TestImport(TestCase):
    def test_1(self):
        from djangocassandra.db.backends.cassandra.base import DatabaseClient

class TestInsertion(TestCase):
    fake_data = [('a', 'b', 'c'), ('d', 'e', 'f'), ('g', 'h', 'i')]
    class InsertionTestModel(Model):
        field_1 = CharField(max_length=50)
        field_2 = CharField(max_length=50)
        field_3 = CharField(max_length=50)

    def test_insertion(self):
        self.connection = connect_db()
        create_db(self.connection, self.InsertionTestModel)
        test_data = []
        for x, y, z in self.fake_data:
            test_data.append(self.InsertionTestModel(field_1=x, field_2=y, field_3=z))
        populate_db(self.connection, test_data)


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
