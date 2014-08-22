from django.db.utils import DatabaseError

from djangotoolbox.db.creation import NonrelDatabaseCreation

from cassandra.metadata import (
    TableMetadata,
    ColumnMetadata,
    IndexMetadata
)

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

from cassandra.util import OrderedDict


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

    '''
    Table Options:

    'comment': None,
    'read_repair_chance': None,
    'dclocal_read_repair_chance': None,
    'replicate_on_write': None,
    'gc_grace_seconds': None,
    'bloom_filter_fp_chance': None,
    'caching': None,
    'compaction_strategy_class': None,
    'compaction_strategy_options': None,
    'min_compaction_threshold': None,
    'max_compaction_threshold': None,
    'compression_parameters': None,
    'min_index_interval': None,
    'max_index_interval': None,
    'index_interval': None,
    'speculative_retry': None,
    'rows_per_partition_to_cache': None,
    'memtable_flush_period_in_ms': None,
    'populate_io_cache_on_flush': None,
    'compaction': None,
    'compression': None,
    'default_time_to_live': None
    '''
    default_table_options = {
    }

    default_cassandra_model_settings = {
        'table_options': default_table_options
    }

    def db_type(
        self,
        field
    ):
        data_type = self.data_types.get(field.get_internal_type())

        if None is data_type:
            return field.db_type(connection=self.connection)

        else:
            return data_type

    def sql_create_model(
        self,
        model,
        style,
        known_models=set()
    ):
        connection_settings = self.connection.settings_dict

        meta = model._meta

        if (
            not meta.managed or
            meta.proxy or
            meta.db_table in known_models
        ):
            return [], {}

        if hasattr(model, 'Cassandra'):
            cassandra_settings = model.Cassandra.__dict__

        else:
            cassandra_settings = self.default_cassandra_model_settings

        if hasattr(cassandra_settings, 'keyspace'):
            keyspace = cassandra_settings.keyspace

        else:
            keyspace = connection_settings.get('DEFAULT_KEYSPACE')

        keyspace_metadata = self.connection._cluster.metadata.keyspaces.get(
            keyspace
        )
        if not keyspace_metadata:
            keyspace_metadata = self.connection._create_keyspace(
                keyspace
            )

        table_options = (
            self.default_table_options.copy()
        )
        if hasattr(cassandra_settings, 'table_options'):
            if not isinstance(cassandra_settings.table_optoins, dict):
                raise DatabaseError(
                    'The value of table_optoins in the Cassandra class '
                    'must be a dict containing overrides for the default'
                    'column family options.'
                )
            table_options.update(cassandra_settings.table_metadata)

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

        return [], {}
