from itertools import chain
from collections import OrderedDict

from django.apps import apps
from django.db.models import (
    Model as DjangoModel,
    Manager,
    ForeignKey
)
from django.db.models.fields import (
    FieldDoesNotExist
)

from .fields import TokenPartitionKeyField
from .values import PrimaryKeyValue
from .query import QuerySet


class ColumnFamilyManager(Manager.from_queryset(QuerySet)):
    denormalized_models = []

    @staticmethod
    def denormalize(
        origin,
        destination
    ):
        field_names = origin._meta.get_all_field_names()
        for name in field_names:
            try:
                field = origin._meta.get_field(name)

            except FieldDoesNotExist:
                continue

            setattr(
                destination,
                field.name,
                getattr(
                    origin,
                    field.name
                )
            )

        return destination

    def create(
        self,
        *args,
        **kwargs
    ):
        instance = super(ColumnFamilyManager, self).create(
            *args,
            **kwargs
        )

        return instance



class ColumnFamilyModel(DjangoModel):
    class Meta:
        abstract = True

    pk_token = TokenPartitionKeyField()

    def __init__(
        self,
        *args,
        **kwargs
    ):
        super(ColumnFamilyModel, self).__init__(
            *args,
            **kwargs
        )

    def __hash__(self):
        pk_value = self._get_pk_val()

        if isinstance(pk_value, dict):
            return int(pk_value)

        else:
            return super(
                ColumnFamilyModel,
                self
            ).__hash__()
        

    def _get_pk_val(self, meta=None):
        if (
            hasattr(self, "Cassandra") and
            hasattr(self.Cassandra, "partition_keys")
        ):
            if hasattr(self.Cassandra, "clustering_keys"):
                all_keys = [key for key in chain(
                    self.Cassandra.partition_keys,
                    self.Cassandra.clustering_keys
                )]

            else:
                all_keys = self.Cassandra.partition_keys

            if 1 < len(all_keys):
                pk_value = PrimaryKeyValue()
                for key in all_keys:
                    field = self._meta.get_field_by_name(key)[0]
                    if isinstance(field, ForeignKey):
                        key += "_id"

                    pk_value[key] = field.get_prep_value(getattr(
                        self,
                        key
                    ))

                return pk_value

        return super(
            ColumnFamilyModel,
            self
        )._get_pk_val(meta=meta)
                
    def _set_pk_val(self, value):
        if (
            hasattr(self, "Cassandra") and
            hasattr(self.Cassandra, "partition_keys")
        ):
            if hasattr(self.Cassandra, "clustering_keys"):
                all_keys = [field for field in chain(
                    self.Cassandra.partition_keys,
                    self.Cassandra.clustering_keys
                )]

            else:
                all_keys = self.Cassandra.partition_keys

            if 1 < len(all_keys):
                if not isinstance(value, dict):
                    raise self.PrimaryKeyInconsistencyException(
                        "PK values must be a dict of the form "
                        "{ field: value, ... }"
                    )

                items = [i for i in value.iteritems()]

                if len(items) != len(pk_fields):
                    raise self.PrimaryKeyInconsistencyException(
                        "Expected %s field/value pairs, recieved %s." % (
                            len(pk_fields),
                            len(items)
                        )
                    )

                for field, value in items:
                    if field not in pk_fields:
                        raise self.PrimaryKeyInconsistencyException((
                            "Field \"%s\" is not part of the primary key."
                            "must be one of %s"
                        ) % (field, pk_fields))

                # Loop twice to avoid partialy updating the key.
                for field, value in items:
                    setattr(self, field, value)

                return None

        return super(
            ColumnFamilyModel,
            self
        )._set_pk_val(value)

    pk = property(_get_pk_val, _set_pk_val)

    @staticmethod
    def should_denormalize(instance):
        '''
        Return false if you don't want to denormalize the
        passed in instance into this column family.
        '''
        return True

    def denormalize(
        self,
        instance
    ):
        ColumnFamilyManager.denormalize(
            instance,
            self
        )

    def save(
        self,
        *args,
        **kwargs
    ):
        kwargs['force_insert'] = True
        kwargs['force_update'] = False

        super(ColumnFamilyModel, self).save(
            *args,
            **kwargs
        )

        if hasattr(
            self._meta.model.objects,
            'denormalized_models'
        ):
            denormalized_models = self._meta.model.objects.denormalized_models

        else:
            denormalized_models = []

        if (
            None is denormalized_models or
            0 >= len(denormalized_models)
        ):
            return

        for model in denormalized_models:
            app_label = None

            if isinstance(model, tuple):
                app_label = model[0]
                model = model[1]

            if isinstance(model, str):
                if None is app_label:
                    app_label = self._meta.app_label

                model = apps.get_model(
                    app_label=app_label,
                    model_name=model
                )

            if (
                not issubclass(model, DjangoModel) or
                isinstance(self, model)
            ):
                continue

            if hasattr(model, 'should_denormalize'):
                if not model.should_denormalize(self):
                    continue

            denormalized_instance = model()
            if hasattr(model, 'denormalize'):
                denormalized_instance.denormalize(
                    self
                )

            else:
                ColumnFamilyManager.denormalize(
                    self,
                    denormalized_instance
                )

            super(ColumnFamilyModel, denormalized_instance).save(
                *args,
                **kwargs
            )

    def delete(
        self,
        *args,
        **kwargs
    ):
        if hasattr(
            self._meta.model.objects,
            'denormalized_models'
        ):
            denormalized_models = self._meta.model.objects.denormalized_models

        else:
            denormalized_models = []

        if (
            None is denormalized_models or
            0 >= len(denormalized_models)
        ):
            super(ColumnFamilyModel, self).delete(
                *args,
                **kwargs
            )
            return

        for model in denormalized_models:
            app_label = None

            if isinstance(model, tuple):
                app_label = model[0]
                model = model[1]

            if isinstance(model, str):
                if None is app_label:
                    app_label = self._meta.app_label

                model = apps.get_model(
                    app_label=app_label,
                    model_name=model
                )

            if (
                not issubclass(model, DjangoModel) or
                isinstance(self, model)
            ):
                continue

            if hasattr(model, 'should_denormalize'):
                if not model.should_denormalize(self):
                    continue

            denormalized_instance = model()
            if hasattr(model, 'denormalize'):
                denormalized_instance.denormalize(
                    self
                )

            else:
                ColumnFamilyManager.denormalize(
                    self,
                    denormalized_instance
                )

            super(
                ColumnFamilyModel,
                denormalized_instance
            ).delete(
                *args,
                **kwargs
            )

        super(ColumnFamilyModel, self).delete(
            *args,
            **kwargs
        )

    objects = ColumnFamilyManager()

    class PrimaryKeyInconsistencyException(Exception):
        pass
