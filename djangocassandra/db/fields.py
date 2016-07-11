import uuid

from django.utils.six import with_metaclass
from django.db.models import (
    Field,
    AutoField,
    SubfieldBase,
    CharField,
    ForeignKey
)
from django.utils.translation import ugettext_lazy as _

from cassandra.cqlengine.functions import Token


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

        return uuid.UUID(value)

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
        value = super(AutoField, self).get_prep_value(value)
        if (
            value is None or
            isinstance(value, uuid.UUID)
        ):
            return value

        return uuid.UUID(value)

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
