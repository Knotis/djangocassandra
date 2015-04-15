.. _usage:

Usage
=====

Djangocassandra is designed to be used with the **stock** version of Django 1.7 (specifically tested with Django 1.7.7). Many features of the Django ORM are supported however there are a few exceptions that should be taken into consideration when using djangocassandra as your database backend.

.. _autofields:

AutoFields
----------

Currently Django only supports integer auto fields. This isn't a huge issue as Cassandra works just fine if you use integer primary keys however when using Cassandra you can use any column type as your primary keys. Here's a few solutions to this problem that you may find useful:

Use The Included AutoFieldUUID Model Field
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

I have included in the djangocassandra project an AutoFieldUUID model field that correctly handles the generation of uuid4() UUID's upon creation. You must specifically define the autofield on all of your models as shown::

  class AutoIdModel(Model):
        id = AutoFieldUUID(primary_key=True)
        data = CharField(
            max_length=64
        )


Use The Knotis Fork Of Django
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Instead of installing stock Django you can install the `Knotis custom-autofield branch <https://github.com/Knotis/django/tree/custom-autofield>`_ (https://github.com/Knotis/django/tree/custom-autofield) of Django which provides you a variable in settings.py to provide your own AutoField like so::

  CUSTOM_AUTOFIELD_CLASS='djangocassandra.db.fields.AutoFieldUUID'

Doing this will make Django use the auto field class you defined whenever it would normally add the stock integer based auto field.

Installing the Knotis fork of Django is as simple as running:

``pip install git+https://github.com/Knotis/django@custom-autofield``
