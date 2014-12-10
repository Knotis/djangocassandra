from cqlengine import columns
from cqlengine.models import (
    Model as CqlEngineModel,
    ModelMetaClass as CqlEngineModelMetaClass
)
from cqlengine.management import create_keyspace

internal_type_to_column_map = {
    'AutoField': columns.UUID,
    'RelatedAutoField': columns.UUID,
    'ForeignKey': columns.UUID,
    'OneToOneField': columns.UUID,
    'ManyToManyField': columns.UUID,
    'BigIntegerField': columns.BigInt,
    'BooleanField': columns.Boolean,
    'CharField': columns.Text,
    'CommaSeparatedIntegerField': columns.Text,
    'DateField': columns.Date,
    'DateTimeField': columns.DateTime,
    'DecimalField': columns.Decimal,
    'EmailField': columns.Text,
    'FileField': columns.Text,
    'FilePathField': columns.Text,
    'FloatField': columns.Float,
    'ImageField': columns.Text,
    'IntegerField': columns.Integer,
    'IPAddressField': columns.Inet,
    'NullBooleanField': columns.Boolean,
    'PositiveIntegerField': columns.Integer,
    'PositiveSmallIntegerField': columns.Integer,
    'SlugField': columns.Text,
    'SmallIntegerField': columns.Integer,
    'TextField': columns.Text,
    'TimeField': columns.Text,
    'URLField': columns.Text,
    'AbstractIterableField': columns.List,
    'ListField': columns.List,
    'SetField': columns.Set,
    'DictField': columns.Map,
    'EmbeddedModelField': columns.Map,
    'RawField': columns.Blob,
    'BlobField': columns.Blob
}


class ColumnFamilyMetaClass(CqlEngineModelMetaClass):
    __column_families__ = {}

    def __new__(
        meta,
        name,
        bases,
        attrs
    ):
        if name in ColumnFamilyMetaClass.__column_families__:
            return ColumnFamilyMetaClass.__column_families__[name]

        model = attrs.get('__model__')
        if None is not model:
            for field in model._meta.fields:
                column_type = internal_type_to_column_map[
                    field.get_internal_type()
                ]
                column = column_type(
                    primary_key=field.primary_key,
                    index=field.db_index,
                    db_field=field.db_column,
                    required=not field.blank
                )
                attrs[field.column] = column

        column_family = super(ColumnFamilyMetaClass, meta).__new__(
            meta,
            name,
            bases,
            attrs
        )

        ColumnFamilyMetaClass.__column_families__[name] = column_family
        return column_family


class ColumnFamily(CqlEngineModel):
    __model__ = None
    __abstract__ = True
    __metaclass__ = ColumnFamilyMetaClass


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


def get_column_family(
    connection,
    model
):
    connection_settings = connection.settings_dict

    if hasattr(model, 'Cassandra'):
        cassandra_settings = model.Cassandra.__dict__

    else:
        cassandra_settings = default_cassandra_model_settings

    if hasattr(cassandra_settings, 'keyspace'):
        keyspace = cassandra_settings.keyspace

    else:
        keyspace = connection_settings.get('DEFAULT_KEYSPACE')

    keyspace_settings = connection_settings.get('KEYSPACES', {}).get(keyspace)
    if None is keyspace_settings:
        keyspace_settings = {}  # Replace with default keyspace settings.

    replication_factor = keyspace_settings['replication_factor']
    replication_strategy_class = keyspace_settings['replication_strategy']

    create_keyspace(
        keyspace,
        replication_strategy_class,
        replication_factor
    )

    table_options = default_table_options

    if hasattr(cassandra_settings, 'table_options'):
        if not isinstance(cassandra_settings.table_optoins, dict):
            raise DatabaseError(
                'The value of table_optoins in the Cassandra class '
                'must be a dict containing overrides for the default'
                'column family options.'
            )
        table_options.update(cassandra_settings.table_metadata)

    return type(
        str(model._meta.db_table),
        (ColumnFamily,), {
            '__model__': model,
            'table_options': table_options
        }
    )
