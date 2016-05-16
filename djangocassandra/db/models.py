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

        for model in self.denormalized_models:
            app_label = None

            if isinstance(model, tuple):
                app_label = model[0]
                model = model[1]

            if isinstance(model, str):
                if None is app_label:
                    app_label = self.model._meta.app_label

                model = apps.get_model(
                    app_label=app_label,
                    model_name=model
                )

            if (
                not issubclass(model, DjangoModel) or
                isinstance(instance, model)
            ):
                continue

            if hasattr(model, 'should_denormalize'):
                if not model.should_denormalize(instance):
                    continue

            denormalized_instance = model()
            if hasattr(model, 'denormalize'):
                denormalized_instance.denormalize(
                    instance
                )

            else:
                ColumnFamilyManager.denormalize(
                    instance,
                    denormalized_instance
                )

            denormalized_instance.save()

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

        return super(ColumnFamilyModel, self).save(
            *args,
            **kwargs
        )

    objects = ColumnFamilyManager()
