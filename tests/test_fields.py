import uuid
from unittest import TestCase

from .models import (
    UUIDFieldModel
)

from .util import (
    connect_db,
    destroy_db,
    create_model
)


class UUIDFieldModelTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()
        self.cached_rows = {}

        create_model(
            self.connection,
            UUIDFieldModel
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_create_uuid_field_model(self):
        instance = UUIDFieldModel.objects.create()

        self.assertIsNotNone(instance)
        uuid.UUID(instance.uuid)
        uuid.UUID(instance.id)

        UUIDFieldModel.objects.get(pk=instance.pk)
