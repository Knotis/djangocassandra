import datetime

from django.db.models import (
    Model,
    CharField,
    BigIntegerField,
    BooleanField,
    CommaSeparatedIntegerField,
    DateField,
    DateTimeField,
    DecimalField,
    EmailField,
    FileField,
    FilePathField,
    FloatField,
    ImageField,
    IntegerField,
    IPAddressField,
    NullBooleanField,
    PositiveIntegerField,
    PositiveSmallIntegerField,
    SlugField,
    SmallIntegerField,
    TextField,
    URLField,
    ForeignKey
)

from djangotoolbox.fields import DictField
from djangocassandra.db.fields import (
    AutoFieldUUID,
    FieldUUID
)
from djangocassandra.db.models import (
    ColumnFamilyModel,
    ColumnFamilyManager
)

from util import (
    random_string,
    random_integer
)


class ColumnFamilyTestModel(ColumnFamilyModel):
    field_1 = CharField(
        primary_key=True,
        max_length=32
    )
    field_2 = CharField(
        max_length=32
    )
    field_3 = CharField(
        max_length=32
    )


class ColumnFamilyIndexedTestModel(ColumnFamilyModel):
    field_1 = CharField(
        primary_key=True,
        max_length=32
    )
    field_2 = CharField(
        max_length=32
    )
    field_3 = CharField(
        max_length=32
    )
    field_4 = CharField(
        max_length=32,
        db_index=True
    )


class SimpleTestModel(Model):
    field_1 = CharField(max_length=32)
    field_2 = CharField(max_length=32)
    field_3 = CharField(max_length=32)


class CustomNameTestModel(SimpleTestModel):
    class Meta:
        db_table = 'custom_model_testtesttest'


class DateTimeTestModel(Model):
    datetime_field = DateTimeField()


class ComplicatedTestModel(Model):
    char_field = CharField(max_length=32)
    bigint_field = BigIntegerField()
    boolean_field = BooleanField()
    commaseparatedinteger_field = CommaSeparatedIntegerField()
    date_field = DateField()
    datetime_field = DateTimeField()
    decimal_field = DecimalField()
    email_field = EmailField()
    file_field = FileField()
    filepath_field = FilePathField(path="/home")
    float_field = FloatField()
    image_field = ImageField()
    integer_field = IntegerField()
    ipaddress_field = IPAddressField()
    nullboolean_field = NullBooleanField()
    positiveinteger_field = PositiveIntegerField()
    positivesmallinteger_field = PositiveSmallIntegerField()
    slug_field = SlugField()
    smallinteger_field = SmallIntegerField()
    text_field = TextField()
    url_field = URLField()

    def auto_populate(self):
        self.char_field = 'foo'
        self.bigint_field = 2379238742398
        self.boolean_field = True
        self.commaseparatedinteger_field = '1, 2, 3, 4'
        self.date_field = datetime.datetime(2014, 1, 1)
        self.datetime_field = datetime.datetime(2014, 1, 1)
        self.decimal_field = 3.14
        self.email_field = 'example@example.com'
        self.file_field = 'test.txt'
        self.filepath_field = 'test.txt'
        self.float_field = 3.14
        self.image_field = 'test.png'
        self.integer_field = 1024
        self.ipaddress_field = '8.8.8.8'
        self.nullboolean_field = None
        self.positiveinteger_field = 1024
        self.positivesmallinteger_field = 1024
        self.slug_field = 'eat-slugs-all-day-long'
        self.smallinteger_field = 3
        self.text_field = 'foo bar var\nvar bar foo'
        self.url_field = 'http://example.com'


class RelatedModelA(Model):
    id = AutoFieldUUID(primary_key=True)
    data = CharField(
        max_length=64
    )


class RelatedModelB(Model):
    id = AutoFieldUUID(primary_key=True)
    model_a = ForeignKey(
        RelatedModelA,
        null=True
    )


class RelatedModelC(Model):
    id = AutoFieldUUID(primary_key=True)
    model_a = ForeignKey(
        RelatedModelA,
        null=True
    )
    model_b = ForeignKey(
        RelatedModelB,
        null=True
    )


class UUIDFieldModel(Model):
    id = AutoFieldUUID(primary_key=True)
    uuid = FieldUUID()


class ClusterPrimaryKeyModel(ColumnFamilyModel):
    class Cassandra:
        clustering_keys = ['field_2', 'field_3']

    field_1 = CharField(
        primary_key=True,
        max_length=32
    )
    field_2 = CharField(max_length=32)
    field_3 = CharField(max_length=32)
    data = CharField(max_length=64)

    def auto_populate(self):
        self.field_1 = random_string(32)
        self.field_2 = random_string(32)
        self.field_3 = random_string(32)
        self.data = random_string(64)


class PartitionPrimaryKeyModel(ColumnFamilyModel):
    class Cassandra:
        partition_keys = ['field_1', 'field_2']
        clustering_keys = ['field_3', 'field_4']

    field_1 = CharField(
        primary_key=True,
        max_length=32
    )
    field_2 = CharField(max_length=32)
    field_3 = CharField(max_length=32)
    field_4 = CharField(max_length=32)
    data = CharField(max_length=64)

    def auto_populate(self):
        self.field_1 = random_string(32)
        self.field_2 = random_string(32)
        self.field_3 = random_string(32)
        self.field_4 = random_string(32)
        self.data = random_string(64)


class AbstractTestModel(Model):
    class Meta:
        abstract = True

    inherited_1 = CharField(
        max_length=32,
        db_index=True
    )
    inherited_2 = CharField(max_length=32)
    pub_date = DateTimeField('date published', auto_now_add=True)


class DerivedPartitionPrimaryKeyModel(AbstractTestModel):
    class Cassandra:
        partition_keys = ['field_1', 'field_2']
        clustering_keys = ['inherited_1', 'pub_date']

    field_1 = CharField(
        primary_key=True,
        max_length=32
    )
    field_2 = CharField(
        max_length=32,
        db_index=True
    )
    data = CharField(
        max_length=128
    )


class DenormalizedModelManager(ColumnFamilyManager):
    denormalized_models = [
        'DenormalizedModelA',
        'Denormalizedmodelb'
    ]


class DenormalizedModelBase(ColumnFamilyModel):
    class Meta:
        abstract = True

    objects = DenormalizedModelManager()

    def auto_populate(self):
        self.field_1 = random_string(16)
        self.field_2 = random_integer()


class DenormalizedModelA(DenormalizedModelBase):
    class Cassandra:
        partition_keys = [
            'field_1'
        ]
        clustering_keys = [
            'created'
        ]

    field_1 = CharField(
        max_length=16,
        primary_key=True
    )
    field_2 = IntegerField()
    created = DateTimeField(
        default=datetime.datetime.utcnow
    )


class DenormalizedModelB(DenormalizedModelBase):
    class Cassandra:
        partition_keys = [
            'field_2'
        ]
        clustering_keys = [
            'created'
        ]

    field_1 = CharField(max_length=16)
    field_2 = IntegerField(primary_key=True)
    created = DateTimeField(
        default=datetime.datetime.utcnow
    )


class ForeignPartitionKeyModel(ColumnFamilyModel):
    class Cassandra:
        partition_keys = [
            'related'
        ]
        clustering_keys = [
            'created'
        ]

    related = ForeignKey(
        ClusterPrimaryKeyModel,
        primary_key=True
    )
    created = DateTimeField(
        default=datetime.datetime.utcnow
    )


class DictFieldModel(ColumnFamilyModel):
    id = AutoFieldUUID(primary_key=True)
    parameters = DictField(CharField(max_length=4096))
