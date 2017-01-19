import uuid

from math import log10, floor
from datetime import datetime
from django.utils.six import with_metaclass
from django.db.models import (
    Field,
    AutoField,
    SubfieldBase,
    CharField,
    ForeignKey,
    DateTimeField as DjangoDateTimeField
)
from django.utils.translation import ugettext_lazy as _

from cassandra.cqlengine.functions import Token

from .values import PrimaryKeyValue


class DateTimeField(DjangoDateTimeField):
    def get_prep_value(self, value):
        # Hack cassandra truncates microseconds to milliseconds.
        microsecond = value.microsecond
        if 0 != microsecond:
            microsecond = round(
                value.microsecond,
                -int(floor(log10(abs(value.microsecond)))) + 2
            )
            value = datetime(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                int(microsecond),
                value.tzinfo
            )

        return super(DateTimeField, self).get_prep_value(value)


class TokenPartitionKeyField(Field):
    def __init__(
        self,
        *args,
        **kwargs
    ):
        update_kwargs = {
            'null': True,
            'blank': True,
            'editable': False,
            'db_column': 'pk__token'
        }

        kwargs.update(update_kwargs)

        super(TokenPartitionKeyField, self).__init__(
            *args,
            **kwargs
        )

    def contribute_to_class(
        self,
        cls,
        name
    ):
        super(TokenPartitionKeyField, self).contribute_to_class(
            cls,
            name
        )

        setattr(cls, name, self)

    def get_internal_type(self):
        return 'Token'

    def __get__(
        self,
        instance,
        instance_type=None
    ):
        if None is instance:
            return self

        if (
            hasattr(instance, 'Cassandra') and
            hasattr(instance.Cassandra, 'partition_keys') and
            1 < len(instance.Cassandra.partition_keys)
        ):
            compound_pk = []
            for key in instance.Cassandra.partition_keys:
                field = instance._meta.get_field(key)
                if isinstance(field, ForeignKey):
                    key += '_id'

                value = getattr(instance, key)
                try:
                    value = uuid.UUID(value)

                except:
                    pass

                compound_pk.append(value)

            pk = tuple(compound_pk)

        else:
            pk = instance.pk
            try:
                pk = uuid.UUID(pk)

            except:
                pass

        return Token(pk)

    def __set__(
        self,
        instance,
        value
    ):
        pass

    def value_to_string(
        self,
        obj
    ):
        value = self._get_val_from_obj(obj)
        return ''.join([
            'token(',
            ','.join(str(value.value)),
            ')'
        ])


class FieldUUID(with_metaclass(SubfieldBase, CharField)):
    description = _('UUID')

    default_error_messages = {
        'invalid': _("'%(value)s' value must be a valid UUID."),
    }

    def __init__(
        self,
        *args,
        **kwargs
    ):
        if 'default' not in kwargs:
            kwargs['default'] = uuid.uuid4

        kwargs['max_length'] = 36

        super(FieldUUID, self).__init__(
            *args,
            **kwargs
        )

    def to_python(
        self,
        value
    ):
        if isinstance(value, uuid.UUID):
            return str(value)

        else:
            return value

    def get_prep_value(self, value):
        value = super(FieldUUID, self).get_prep_value(value)
        if (
            value is None or
            isinstance(value, uuid.UUID)
        ):
            return value

        try:
            return uuid.UUID(value)

        except:
            return value

    def get_internal_type(self):
        return 'FieldUUID'

    @staticmethod
    def get_auto_value(self):
        return uuid.uuid4()

    def value_to_string(self, value):
        if isinstance(value, basestring):
            return value

        try:
            return str(value)

        except (TypeError, ValueError):
            raise Exception(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )


class AutoFieldUUID(with_metaclass(SubfieldBase, AutoField)):
    description = _('UUID')

    default_error_messages = {
        'invalid': _("'%(value)s' value must be a valid UUID."),
    }

    def __init__(
        self,
        *args,
        **kwargs
    ):
        if 'default' not in kwargs:
            kwargs['default'] = uuid.uuid4

        super(AutoFieldUUID, self).__init__(
            *args,
            **kwargs
        )

    def to_python(
        self,
        value
    ):
        if isinstance(value, uuid.UUID):
            return str(value)

        else:
            return value

    def get_prep_value(self, value):
        if (
            value is None or
            isinstance(value, uuid.UUID)
        ):
            return value

        try:
            return uuid.UUID(value)

        except:
            return value

    def get_internal_type(self):
        return 'AutoFieldUUID'

    @staticmethod
    def get_auto_value(self):
        return uuid.uuid4()

    def value_to_string(self, value):
        if isinstance(value, basestring):
            return value

        try:
            return str(value)

        except (TypeError, ValueError):
            raise Exception(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )


class PrimaryKeyField(Field):
    def __init__(
        self,
        *args,
        **kwargs
    ):
        self.field_class = kwargs.pop(
            'field_class',
            AutoFieldUUID
        )
        self.field_args = args
        self.field_kwargs = kwargs

        super(PrimaryKeyField, self).__init__()

    def contribute_to_class(
        self,
        cls,
        name
    ):
        self.name = name

        if (
            hasattr(cls, "Cassandra") and
            hasattr(cls.Cassandra, "partition_keys")
        ):
            if hasattr(cls.Cassandra, "clustering_keys"):
                self.all_keys = [key for key in chain(
                    cls.Cassandra.partition_keys,
                    cls.Cassandra.clustering_keys
                )]

            else:
                self.all_keys = cls.Cassandra.partition_keys

        class PrimaryKeyField(self.field_class):
            def __init__(self, *args, **kwargs):
                kwargs["primary_key"] = True

                super(PrimaryKeyField, self).__init__(
                    *args,
                    **kwargs
                )

            def get_prep_value(self, value):
                if isinstance(value, PrimaryKeyValue):
                    return value

                elif isinstance(self, ForeignKey):
                    return self.related_field.get_prep_value(value)
                    
                else:
                    return super(PrimaryKeyField, self).get_prep_value(value)

            def get_internal_type(self):
                if isinstance(self, ForeignKey):
                    return "ForeignKey"

                return super(
                    PrimaryKeyField,
                    self
                ).get_internal_type()

        cls.add_to_class(
            name,
            PrimaryKeyField(
                *self.field_args,
                **self.field_kwargs
            )
        )
