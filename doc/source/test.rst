.. _test:

Testing
=======

Environment Variables
---------------------

DJANGOCASSANDRA_TEST_HOST
    *The ipaddress or hostname of your cassandra cluster (default: "localhost")*


DJANGOCASSANDRA_TEST_PORT
    *The port that cassandra is listening on (default: "9042")*


For additional configuration see tests/settings.py
    
Running the Tests
-----------------

I like to use nose but you can use whatever test runner you are into.

Installing Nose
^^^^^^^^^^^^^^^

    | ``pip install nose``
    | ``export DJANGO_SETTINGS_MODULE=tests.settings``

After installing nose run:

    nosetests

to run the tests.
