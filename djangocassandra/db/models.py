from django.apps import apps
from django.db.models import (
    Model as DjangoModel,
    Manager
)
from django.db.models.fields import (
    FieldDoesNotExist
)

from .fields import TokenPartitionKeyField
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
