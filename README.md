# The Django Cassandra Database Backend

The Cassandra database backend for Django has been developed to allow developers to utilize the Apache Cassandra Database in their Django projects.

## TL;DR

* Compatable with vanilla Django 1.7 (1.8 comming soon).
* Use the native Django ORM to define your models.
* SchemaEditor has been implemented to support Django migration management commands (migrate/makemigrations).
* Utilizes The Datastax Cassandra Python Driver for communicating with Cassandra.
* Provides in-memory (inefficient) query support for maximum compatibilty with Django functionality and third party libraries.

## Configuring a Test Environment

### Install Apache Cassandra

Option 1: https://wiki.apache.org/cassandra/GettingStarted

Option 2: http://docs.datastax.com/en/cassandra/2.0/cassandra/install/install_cassandraTOC.html

### Install Djangocassandra

If you want to set up a virtual environment seperate from your normal python install run:

    virtualenv venv
    source venv/bin/activate

This sets up your current path and environment variables to point at your newly created virtual environment named "venv".

To install Djangocassandra run:

    pip install --upgrade ./

### Environment Variables

**DJANGOCASSANDRA_TEST_HOST** - *The ipaddress or hostname of your cassandra cluster (default: "localhost")*


**DJANGOCASSANDRA_TEST_PORT** - *The port that cassandra is listening on (default: "9042")*


For additional configuration see tests/settings.py
    
## Running the Tests

I like to use nose but you can use whatever test runner you are into.

### Installing Nose

    pip install nose 
    export DJANGO_SETTINGS_MODULE=tests.settings

After installing nose run:

    nosetests

to run the tests.
