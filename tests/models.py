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

from djangocassandra.db.fields import AutoFieldUUID
from djangocassandra.db.models import ColumnFamilyModel


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


class SimpleTestModel(Model):
    field_1 = CharField(max_length=32)
    field_2 = CharField(max_length=32)
    field_3 = CharField(max_length=32)


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


class ClusterPrimaryKeyModel(Model):
    class CassandraMeta:
        clustering_keys = ['field_2', 'field_3']

    field_1 = CharField(
        primary_key=True,
        max_length=32
    )
    field_2 = CharField(max_length=32)
    field_3 = CharField(max_length=32)
    data = CharField(max_length=64)
