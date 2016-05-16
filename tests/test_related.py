from unittest import TestCase

from .models import (
    RelatedModelA,
    RelatedModelB,
    RelatedModelC
)

from .util import (
    connect_db,
    destroy_db,
    create_model,
    random_string
)


class RelatedModelCreationTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_create_related_models(self):
        create_model(
            self.connection,
            RelatedModelA
        )
        create_model(
            self.connection,
            RelatedModelB
        )
        create_model(
            self.connection,
            RelatedModelC
        )

        objA = RelatedModelA.objects.create(data='FooBarLel')
        objB = RelatedModelB.objects.create(model_a=objA)
        RelatedModelC.objects.create(
            model_a=objA,
            model_b=objB
        )


class RelatedModelQueryTestCase(TestCase):
    def setUp(self):
        self.connection = connect_db()

        self.cached_rows = {}

        create_model(
            self.connection,
            RelatedModelA
        )
        create_model(
            self.connection,
            RelatedModelB
        )
        create_model(
            self.connection,
            RelatedModelC
        )

        import django
        django.setup()

    def tearDown(self):
        destroy_db(self.connection)

    def test_in_filter(self):
        pks = []
        for i in xrange(10):
            pks.append(RelatedModelA.objects.create(
                data=random_string()
            ).pk)

        qs = RelatedModelA.objects.filter(pk__in=pks)
        self.assertEqual(len(pks), len(qs))

    def test_related_query(self):
        obj_a0 = RelatedModelA.objects.create(
            data='MadFooBar'
        )

        obj_b0 = RelatedModelB.objects.create(
            model_a=obj_a0
        )

        obj_c0 = RelatedModelC.objects.create(
            model_a=obj_a0,
            model_b=obj_b0
        )

        obj_a0_stored = RelatedModelA.objects.get(
            pk=obj_a0.pk
        )

        obj_b0_stored = RelatedModelB.objects.get(
            pk=obj_b0.pk
        )

        obj_c0_stored = RelatedModelC.objects.get(
            pk=obj_c0.pk
        )

        self.assertEqual(
            obj_a0.data,
            obj_a0_stored.data
        )
        self.assertEqual(
            obj_b0.model_a.data,
            obj_b0_stored.model_a.data
        )
        self.assertEqual(
            obj_c0.model_a.data,
            obj_c0_stored.model_a.data
        )
        self.assertEqual(
            obj_c0.model_b.model_a.data,
            obj_c0_stored.model_b.model_a.data
        )
