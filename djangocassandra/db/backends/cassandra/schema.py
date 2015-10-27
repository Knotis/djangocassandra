from cassandra.cqlengine import management as db_management

from django.db.backends.schema import (
    BaseDatabaseSchemaEditor
)

from djangocassandra.db.meta import (
    CqlColumnFamilyMetaClass,
    get_column_family,
    internal_type_to_column_map
)


class CassandraSchemaEditor(BaseDatabaseSchemaEditor):
    known_models = set()

    def skip_default(
        self,
        field
    ):
        return True

    def _create_db_table(
        self,
        column_family
    ):
        db_management.sync_table(column_family)

    def create_model(
        self,
        model
    ):
        self.connection.create_keyspace()
        column_family = get_column_family(
            self.connection,
            model
        )
        self._create_db_table(column_family)

    def delete_model(
        self,
        model
    ):
        column_family = get_column_family(
            self.connection,
            model
        )

        db_management.drop_table(column_family)

    def alter_unique_together(
        self,
        model,
        old_unique_together,
        new_unique_together
    ):
        '''
        Unique together isn't a concept Cassandra understands
        out of the box. We will just do a noop in this case.
        '''
        pass

    def alter_index_together(
        self,
        model,
        old_index_together,
        new_index_together
    ):
        '''
        Index together is a similar concept to Cassandra's compound
        primary keys but to maintain compatibilty with Django projects
        that are migrating from some other database backend I will keep
        these concepts seperate instead of trying to overload
        _meta.index_together
        '''
        pass

    def alter_db_table(
        self,
        model,
        old_db_table,
        new_db_table
    ):
        '''
        Renaming the table isn't supported by Cassandra. This
        has to be done in a manual process as follows:

            1) create schema for NEW_Keyspace
            2) stop writes to OLD_Keyspace from app (reads can continue)
            3) flush OLD_Keyspace on every node, via nodetool
            4) hard link all sstables from OLD_Keyspace directory to
               NEW_Keyspace directory
            5) call nodetool -h localhost refresh NEW_Keyspace
            6) enable reads/writes from/to NEW_Keyspace from app
               (disable reads on OLD_Keyspace)
            7) clean up OLD_Keyspace (drop schema, delete files, etc.)
        '''
        if (
            new_db_table._meta.db_table !=
            old_db_table._meta.db_table
        ):
            raise Exception('Renaming models not supported')

        self.create_model(new_db_table)

    def alter_db_tablespace(
        self,
        model,
        old_db_tablespace,
        new_db_tablespace
    ):
        '''
        Moving a model to a different keyspace requires
        creating the column family in the new keyspace
        (new_db_tablespace), migrating the data and then
        deleting the old column family.
        '''
        pass

    def add_field(
        self,
        model,
        field
    ):
        column_family = self._add_field(
            model,
            field
        )
        self._create_db_table(column_family)

    def _add_field(
        self,
        model,
        field,
        column_family=None
    ):
        if None is column_family:
            column_family = get_column_family(
                self.connection,
                model
            )

        db_field = (
            field.db_column
            if field.db_column
            else field.column
        )
        cql_field_type = internal_type_to_column_map[field.get_internal_type()]
        cql_field = cql_field_type(
            primary_key=field.primary_key,
            index=field.db_index,
            db_field=db_field,
            required=not field.blank
        )
        setattr(column_family, db_field, cql_field)
        column_family._columns[db_field] = cql_field

        CqlColumnFamilyMetaClass.update_column_family(column_family)
        return column_family

    def remove_field(
        self,
        model,
        field
    ):
        column_family = self._remove_field(
            model,
            field
        )
        self._create_db_table(column_family)

    def _remove_field(
        self,
        model,
        field,
        column_family=None
    ):
        if None is column_family:
            column_family = get_column_family(
                self.connection,
                model
            )

        delattr(column_family, field.name)
        db_field = (
            field.db_column
            if field.db_column
            else field.column
        )
        del column_family._columns[db_field]

        CqlColumnFamilyMetaClass.update_column_family(column_family)
        return column_family

    def alter_field(
        self,
        model,
        old_field,
        new_field,
        strict=False
    ):
        '''
        A little more care needs to be taken here to handle
        cases where exceptions are raised in either _remove_field
        or in _add_field. This could leave the column_family
        registery dirty and in a state that would require the
        web service to restart to function properly.
        '''
        column_family = get_column_family(
            self.connection,
            model
        )
        self._remove_field(
            model,
            old_field,
            column_family=column_family
        )
        self._add_field(
            model,
            new_field,
            column_family=column_family
        )

        self._create_db_table(column_family)
