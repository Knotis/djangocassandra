from djangotoolbox.db.creation import NonrelDatabaseCreation

from cassandra.cqltypes import (
    UUIDType,
    LongType,
    BooleanType,
    VarcharType,
    DateType,
    TimestampType,
    DecimalType,
    FloatType,
    IntegerType,
    InetAddressType,
    Int32Type,
    UTF8Type,
    ListType,
    SetType,
    MapType,
    BytesType
)

from cqlengine.management import (
    sync_table
)

from djangocassandra.db.models import get_column_family


class DatabaseCreation(NonrelDatabaseCreation):
    data_typename_to_typeclass = {
        UUIDType.typename: UUIDType,
        LongType.typename: LongType,
        BooleanType.typename: BooleanType,
        VarcharType.typename: VarcharType,
        DateType.typename: DateType,
        TimestampType.typename: TimestampType,
        DecimalType.typename: DecimalType,
        FloatType.typename: FloatType,
        IntegerType.typename: IntegerType,
        InetAddressType.typename: InetAddressType,
        Int32Type.typename: Int32Type,
        UTF8Type.typename: UTF8Type,
        ListType.typename: ListType,
        SetType.typename: SetType,
        MapType.typename: MapType,
        BytesType.typename: BytesType
    }

    data_types = {
        # NoSQL databases often have specific concepts of entity keys.
        # For example, GAE has the db.Key class, MongoDB likes to use
        # ObjectIds, Redis uses strings, while Cassandra supports
        # different types (including binary data).
        'AutoField': UUIDType.typename,
        'RelatedAutoField': UUIDType.typename,
        'ForeignKey': UUIDType.typename,
        'OneToOneField': UUIDType.typename,
        'ManyToManyField': UUIDType.typename,

        # Standard field types, more or less suitable for a database
        # (or its client / driver) being able to directly store or
        # process Python objects.
        'BigIntegerField': LongType.typename,
        'BooleanField': BooleanType.typename,
        'CharField': VarcharType.typename,
        'CommaSeparatedIntegerField': VarcharType.typename,
        'DateField': DateType.typename,
        'DateTimeField': TimestampType.typename,
        'DecimalField': DecimalType.typename,
        'EmailField': VarcharType.typename,
        'FileField': VarcharType.typename,
        'FilePathField': VarcharType.typename,
        'FloatField': FloatType.typename,
        'ImageField': VarcharType.typename,
        'IntegerField': IntegerType.typename,
        'IPAddressField': InetAddressType.typename,
        'NullBooleanField': BooleanType.typename,
        'PositiveIntegerField': IntegerType.typename,
        'PositiveSmallIntegerField': Int32Type.typename,
        'SlugField': VarcharType.typename,
        'SmallIntegerField': Int32Type.typename,
        'TextField': UTF8Type.typename,
        'TimeField': TimestampType.typename,
        'URLField': VarcharType.typename,
        'BinaryField': BytesType.typename,
        # You may use "list" for SetField, or even DictField and
        # EmbeddedModelField (if your database supports nested lists).
        # All following fields also support "string" and "bytes" as
        # their storage types -- which work by serializing using pickle
        # protocol 0 or 2 respectively.
        # Please note that if you can't support the "natural" storage
        # type then the order of field values will be undetermined, and
        # lookups or filters may not work as specified (e.g. the same
        # set or dict may be represented by different lists, with
        # elements in different order, so the same two instances may
        # compare one way or the other).
        'AbstractIterableField': ListType.typename,
        'ListField': ListType.typename,
        'SetField': SetType.typename,
        'DictField': MapType.typename,
        'EmbeddedModelField': MapType.typename,
        # RawFields ("raw" db_type) are used when type is not known
        # (untyped collections) or for values that do not come from
        # a field at all (model info serialization), only do generic
        # processing for them (if any). On the other hand, anything
        # using the "bytes" db_type should be converted to a database
        # blob type or stored as binary data.
        'RawField': BytesType.typename,
        'BlobField': BytesType.typename
    }

    def db_type(
        self,
        field
    ):
        '''
        TODO:
        Investigate wether this code is redundant.
        After looking at Field.db_type() it seems like
        the code here is duplicated.
        '''
        data_type = self.data_types.get(field.get_internal_type())

        if None is data_type:
            return field.db_type(connection=self.connection)

        else:
            return data_type

    def sql_create_model(
        self,
        model,
        style,  # Used for styling output
        known_models=set()
    ):
        meta = model._meta

        if (
            not meta.managed or
            meta.proxy or
            meta.db_table in known_models
        ):
            return [], {}

        column_family = get_column_family(
            self.connection,
            model
        )
        sync_table(column_family)

        return [], {}

    def _create_test_db(
        self,
        verbosity,
        autoclobber,
        keepdb=False
    ):
        test_database_name = self._get_test_db_name()

        qn = self.connection.ops.quote_name

        nodb_connection = self._nodb_connection

        nodb_connection.create_keyspace(
            qn(test_database_name)
        )

        return test_database_name

'''

        table_metadata = TableMetadata(
            keyspace_metadata,
            meta.db_table,
            options=table_options
        )

        partition_key = []
        clustering_key = []
        primary_key_field = None
        columns = OrderedDict()
        for field in meta.local_fields:
            column_name = field.db_column if field.db_column else field.column
            data_typename = self.data_types.get(
                field.get_internal_type(),
                BytesType.typename
            )
            data_type = self.data_typename_to_typeclass.get(
                data_typename,
                BytesType
            )
            if issubclass(data_type, ListType):
                item_typename = field.item_field.get_internal_type()
                subtype_typename = self.data_types.get(
                    item_typename,
                    BytesType.typename
                )
                subtype = self.data_typename_to_typeclass.get(
                    subtype_typename,
                    BytesType
                )
                data_type = type(
                    'ListType_' + subtype_typename,
                    (data_type,), {
                        'subtypes': (subtype,)
                    }
                )

            elif issubclass(data_type, MapType):
                pass

            column = ColumnMetadata(
                table_metadata,
                column_name,
                data_type
            )

            if field.primary_key:
                assert(None is primary_key_field)
                primary_key_field = field

            if field.db_index:
                index = IndexMetadata(
                    column,
                    index_name='_'.join([
                        'idx',
                        meta.db_table,
                        column_name
                    ])
                )
                column.index = index

            partition_key_columns = cassandra_settings.get(
                'partition_key_columns',
                []
            )
            if field.primary_key or column_name in partition_key_columns:
                partition_key.append(column)

            clustering_key_columns = cassandra_settings.get(
                'clustering_key_columns',
                []
            )
            if column_name in clustering_key_columns:
                clustering_key.append(column)

            columns[column_name] = column

        if columns:
            table_metadata.columns = columns

        assert(len(partition_key) > 0)
        assert(partition_key[0].name == primary_key_field.column)

        if partition_key:
            table_metadata.partition_key = partition_key
        if clustering_key:
            table_metadata.clustering_key = clustering_key

        session = self.connection.get_session(keyspace=keyspace)
        session.execute(table_metadata.as_cql_query())

'''
