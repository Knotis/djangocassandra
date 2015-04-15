# The Django Cassandra Database Backend

The Cassandra database backend for Django has been developed to allow developers to utilize the Apache Cassandra Database in their Django projects.

View the full documentation at https://djangocassandra.readthedocs.org

## TL;DR

* Compatable with vanilla Django 1.7 (1.8 comming soon).
* Use the native Django ORM to define your models.
* SchemaEditor has been implemented to support Django migration management commands (migrate/makemigrations).
* Utilizes The Datastax Cassandra Python Driver for communicating with Cassandra.
* Provides in-memory (inefficient) query support for maximum compatibilty with Django functionality and third party libraries.

## Quick Start - Configuring A Test Environment

see: https://djangocassandra.readthedocs.org#quickstart
