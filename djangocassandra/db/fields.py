import uuid

from django.db.models import AutoField


class AutoFieldUUID(AutoField):
    def to_python(
        self,
        value
    ):
        if (
            value is None or
            isinstance(value, uuid.UUID)
        ):
            return value

        try:
            return uuid.UUID(value)
        except (TypeError, ValueError):
            raise exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def get_prep_value(self, value):
        value = super(AutoField, self).get_prep_value(value)
        if (
            value is None or
            isinstance(value, uuid.UUID)
        ):
            return value

        return uuid.UUID(value)
