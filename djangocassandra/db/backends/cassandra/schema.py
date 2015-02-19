from django.db.backends.schema import (
    BaseDatabaseSchemaEditor
)


class CassandraSchemaEditor(BaseDatabaseSchemaEditor):
    known_models = set()

    def create_model(
        self,
        model
    ):
        self.connection.creation.sql_create_model(
            model,
            None,
            known_models=self.known_models
        )
        if model._meta.db_table not in self.known_models:
            self.known_models.add(model._meta.db_table)
