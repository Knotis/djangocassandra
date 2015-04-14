.. djangocassandra documentation master file, created by
   sphinx-quickstart on Tue Apr 14 05:25:20 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 2

.. _index:

Djangocassandra - The Django Cassnadra Database Backend
=======================================================

.. _overview:

Overview
--------

**Have you ever thought?:**

    "Jeez, the Djano ORM is really nice. I wish that I could use it with a cool NoSql databse like Apache Cassandra."

Finally the dream is real! Welcome to the first Django Database backend for Apache Cassandra that allows you to use the *vanilla* Django 1.7 ORM (1.8 support coming soon) to define and manipulate your database schema including a SchemEditor that enables the use of the migrate and makemigrations management commands that were added in Django 1.7.

Feature Highlights
^^^^^^^^^^^^^^^^^^

    * Compatable with *vanilla* Django 1.7 (1.8 coming soon).
    * Use the native Django ORM to define your models.
    * SchemaEditor has been implemented to support Django migration management commands (migrate/makemigrations).
    * Utilizes The Datastax Cassandra Python Driver for communicating with Cassandra.
    * Provides in-memory (inefficient) query support for maximum compatibilty with Django functionality and third party libraries.

Documentation
-------------

Follow the links bellow to get started using Cassandra with your Django projects:

    | :ref:`quickstart`
    | :ref:`prerequisites`
    | :ref:`settings`
    | :ref:`usage`
    | :ref:`test`
    | :ref:`contribute`     
    | :ref:`search`
