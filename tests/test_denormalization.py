from unittest import TestCase
from datetime import datetime

from .models import (
    DenormalizedModelA,
    DenormalizedModelB
)

from .util import (
    connect_db,
    destroy_db,
    create_model,
    random_string,
    random_integer
)


class DenormalizationTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        create_model(
            self.connection,
            DenormalizedModelA
        )
        create_model(
            self.connection,
            DenormalizedModelB
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_model_denormalization(self):
        instance_a = DenormalizedModelA.objects.create(
            field_1=random_string(16),
            field_2=random_integer(maximum=999999)
        )

        other_instances = []
        for _ in xrange(10):
            other_instances.append(
                DenormalizedModelA.objects.create(
                    field_1=instance_a.field_1,
                    field_2=random_integer(maximum=999999)
                )
            )

        instance_a = DenormalizedModelA.objects.get(
            field_1=instance_a.field_1,
            field_2=instance_a.field_2,
            created=instance_a.created
        )
        instance_b = DenormalizedModelB.objects.get(
            field_1=instance_a.field_1,
            field_2=instance_a.field_2,
            created=instance_a.created
        )

        self.assertEqual(
            instance_a.field_1,
            instance_b.field_1
        )
        self.assertEqual(
            instance_a.field_2,
            instance_b.field_2
        )
        self.assertEqual(
            instance_a.created,
            instance_b.created
        )

        instance_b.delete()

        try:
            deleted_instance_a = DenormalizedModelA.objects.get(
                field_1=instance_a.field_1,
                field_2=instance_a.field_2,
                created=instance_a.created
            )
            self.assertIsNone(deleted_instance_a)

        except DenormalizedModelA.DoesNotExist:
            pass

        try:
            deleted_instance_b = DenormalizedModelB.objects.get(
                field_1=instance_a.field_1,
                field_2=instance_a.field_2,
                created=instance_a.created
            )
            self.assertIsNone(deleted_instance_b)

        except DenormalizedModelB.DoesNotExist:
            pass

        self.assertEqual(
            len(other_instances),
            len(DenormalizedModelA.objects.all())
        )

        self.assertEqual(
            len(other_instances),
            len(DenormalizedModelB.objects.all())
        )
