from collections import OrderedDict

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import (
    Model as CqlEngineModel,
    ModelMetaClass as CqlEngineModelMetaClass
)

from djangotoolbox.fields import (
    ListField,
    SetField,
    DictField
)

from .exception import DatabaseError


internal_type_to_column_map = {
    'AutoField': columns.Integer,
    'AutoFieldUUID': columns.UUID,
    'FieldUUID': columns.UUID,
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
            field.rel.to._meta.pk.get_internal_type()
        )

    return internal_type_to_column_map[
        internal_type
    ]


class CqlColumnFamilyMetaClass(CqlEngineModelMetaClass):
    __column_families__ = {}

    @staticmethod
    def update_column_family(column_family):
        assert issubclass(column_family, CqlEngineModel)
        CqlColumnFamilyMetaClass.__column_families__[
            column_family.__name__
        ] = column_family

    def __new__(
        meta,
        name,
        bases,
        attrs
    ):
        ordered_attrs = OrderedDict(attrs)

        if name in CqlColumnFamilyMetaClass.__column_families__:
            return CqlColumnFamilyMetaClass.__column_families__[name]

        model = attrs.get('__model__')
        registered_model = model
        try:
            from django.apps import apps
            registered_model = apps.get_model(
                model._meta.app_label,
                model._meta.model_name
            )

        except:
            pass

        if hasattr(registered_model, 'Cassandra'):
            cassandra_options = registered_model.Cassandra

        else:
            cassandra_options = None

        if None is not model:
            '''
            Create partition/clustering keys first.
            '''
            partition_keys = []
            if (
                cassandra_options and
                hasattr(cassandra_options, 'partition_keys')
            ):
                partition_keys = cassandra_options.partition_keys

            primary_field_name = None

            if partition_keys:
                for column_name in partition_keys:
                    field = model._meta.get_field(column_name)
                    field_name = (
                        field.db_column if
                        field.db_column else
                        field.column
                    )

                    column_type = get_cql_column_type(field)
                    column = column_type(
                        primary_key=True,
                        partition_key=True,
                        db_field=field_name
                    )
                    ordered_attrs[
                        field.db_column if
                        field.db_column else
                        field.column
                    ] = column

            else:
                primary_key_field = model._meta.pk
                primary_field_name = (
                    primary_key_field.db_column
                    if primary_key_field.db_column
                    else primary_key_field.column
                )

                column_type = get_cql_column_type(primary_key_field)

                column = column_type(
                    primary_key=True,
                    partition_key=True,
                    db_field=primary_field_name
                )
                ordered_attrs[
                    primary_key_field.db_column if
                    primary_key_field.db_column else
                    primary_key_field.column
                ] = column

            clustering_keys = []
            if (
                cassandra_options and
                hasattr(cassandra_options, 'clustering_keys')
            ):
                clustering_keys = cassandra_options.clustering_keys

                for column_name in clustering_keys:
                    field = model._meta.get_field(column_name)
                    field_name = (
                        field.db_column if
                        field.db_column else
                        field.column
                    )

                    if (
                        column_name in partition_keys or
                        field_name == primary_field_name
                    ):
                        # Skip primary key or partition keys if included here.
                        continue

                    column_type = get_cql_column_type(field)
                    column = column_type(
                        primary_key=True,
                        db_field=field_name
                    )
                    ordered_attrs[
                        field.db_column if
                        field.db_column else
                        field.column
                    ] = column

            for field in model._meta.fields:
                if 'Token' == field.get_internal_type():
                    continue

                field_name = (
                    field.db_column if
                    field.db_column else
                    field.column
                )
                if (
                    field.primary_key or
                    field.name in partition_keys or
                    field.name in clustering_keys
                ):
                    continue

                column_type = get_cql_column_type(field)

                args = []
                if (
                    isinstance(field, DictField)
                ):
                    args.append(columns.Text)
                    args.append(
                        get_cql_column_type(field.item_field)
                    )

                elif (
                    isinstance(field, ListField) or
                    isinstance(field, SetField)
                ):
                    args.append(
                        get_cql_column_type(field.item_field)
                    )

                column = column_type(
                    index=field.db_index,
                    db_field=field_name,
                    required=not field.blank,
                    *args
                )

                ordered_attrs[field_name] = column

        column_family = super(CqlColumnFamilyMetaClass, meta).__new__(
            meta,
            name,
            bases,
            ordered_attrs
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

    registered_model = model
    try:
        from django.apps import apps
        registered_model = apps.get_model(
            model._meta.app_label,
            model._meta.model_name
        )

    except:
        pass

    if hasattr(registered_model, 'Cassandra'):
        cassandra_options = registered_model.Cassandra

    else:
        cassandra_options = default_cassandra_model_settings

    if hasattr(cassandra_options, 'keyspace'):
        keyspace = cassandra_options.keyspace

    else:
        keyspace = connection_settings.get('DEFAULT_KEYSPACE')

    keyspace_settings = connection_settings.get('KEYSPACES', {}).get(keyspace)
    if None is keyspace_settings:
        keyspace_settings = {}  # Replace with default keyspace settings.

    table_options = default_table_options

    if hasattr(cassandra_options, 'table_options'):
        if not isinstance(cassandra_options.table_options, dict):
            raise DatabaseError(
                'The value of table_options in the Cassandra class '
                'must be a dict containing overrides for the default'
                'column family options.'
            )
        table_options.update(cassandra_options.table_options)

    return type(
        str(model._meta.db_table),
        (CqlColumnFamily,), {
            '__model__': model,
            '__keyspace__': keyspace,
            'table_options': table_options
        }
    )
