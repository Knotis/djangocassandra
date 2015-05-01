from cassandra.cqlengine import columns
from cassandra.cqlengine.models import (
    Model as CqlEngineModel,
    ModelMetaClass as CqlEngineModelMetaClass
)
from cassandra.cqlengine.management import create_keyspace

from .fields import AutoFieldUUID


internal_type_to_column_map = {
    'AutoField': columns.Integer,
    'AutoFieldUUID': columns.UUID,
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
    'TimeField': columns.DateTime,
    'URLField': columns.Text,
    'AbstractIterableField': columns.List,
    'ListField': columns.List,
    'SetField': columns.Set,
    'DictField': columns.Map,
    'EmbeddedModelField': columns.Map,
    'RawField': columns.Blob,
    'BlobField': columns.Blob
}


def get_cql_column_type(field):
    internal_type = field.get_internal_type()
    if internal_type == 'ForeignKey':
        internal_type = (
            field.related.model._meta.pk.get_internal_type()
        )

    return internal_type_to_column_map[
        internal_type
    ]


class CqlColumnFamilyMetaClass(CqlEngineModelMetaClass):
    __column_families__ = {}

    @staticmethod
    def update_column_family(column_family):
        assert issubclass(column_family, CqlEngineModel)
        CqlColumnFamilyMetaClass.__column_families__[column_family.__name__] = column_family

    def __new__(
        meta,
        name,
        bases,
        attrs
    ):
        if name in CqlColumnFamilyMetaClass.__column_families__:
            return CqlColumnFamilyMetaClass.__column_families__[name]

        model = attrs.get('__model__')
        if hasattr(model, 'CassandraMeta'):
            cassandra_options = model.CassandraMeta

        else:
            cassandra_options = None

        if None is not model:
            '''
            Create primary/clustering keys first. 
            '''
            primary_key_field = model._meta.pk
            column_type = get_cql_column_type(primary_key_field)

            column = column_type(
                primary_key=True
            )
            attrs[
                primary_key_field.db_column if
                primary_key_field.db_column else
                primary_key_field.column
            ] = column

            clustering_keys = []
            if cassandra_options and hasattr(cassandra_options, 'clustering_keys'):
                clustering_keys = cassandra_options.clustering_keys

                for column_name in cassandra_options.clustering_keys:
                    field = model._meta.get_field(column_name)
                    if field.column == primary_key_field.column:
                        # Skip primary key if it was included in the clustering keys.
                        continue

                    column_type = get_cql_column_type(field)
                    column = column_type(
                        primary_key=True
                    )
                    attrs[
                        field.db_column if
                        field.db_column else
                        field.column
                    ] = column
            
            for field in model._meta.fields:
                field_name = (
                    field.db_column if
                    field.db_column else
                    field.column
                )
                if field.primary_key or field_name in clustering_keys:
                    continue
                
                column_type = get_cql_column_type(field)
                column = column_type(
                    index=field.db_index,
                    db_field=field.db_column,
                    required=not field.blank
                )
                attrs[
                    field.db_column if
                    field.db_column else
                    field.column
                ] = column

        column_family = super(CqlColumnFamilyMetaClass, meta).__new__(
            meta,
            name,
            bases,
            attrs
        )

        CqlColumnFamilyMetaClass.__column_families__[name] = column_family
        return column_family


class CqlColumnFamily(CqlEngineModel):
    __model__ = None
    __abstract__ = True
    __metaclass__ = CqlColumnFamilyMetaClass


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

    replication_factor = keyspace_settings.get(
        'replication_factor',

    )

    replication_strategy_class = keyspace_settings['strategy_class']

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
        (CqlColumnFamily,), {
            '__model__': model,
            'table_options': table_options
        }
    )
