from django.db.models import (
    Model as DjangoModel,
    Manager
)

from .fields import TokenPartitionKeyField
from .query import QuerySet


class ColumnFamilyManager(Manager.from_queryset(QuerySet)):
    pass


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

    objects = ColumnFamilyManager()
