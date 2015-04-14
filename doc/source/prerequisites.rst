.. _prerequisites:

Installation and Configuration
==============================

Installing Cassandra
--------------------

    * `Option #1: Apache Instructions <https://wiki.apache.org/cassandra/GettingStarted>`_
    * `Option #2: Datastax Instructions <http://docs.datastax.com/en/cassandra/2.0/cassandra/install/install_cassandraTOC.html>`_

Dependencies
------------

Djangotoolbox
^^^^^^^^^^^^^

The main `djangotoolbox <https://github.com/django-nonrel/djangotoolbox>`_ project is a little behind on merging pull requests. Currently there is an `open pull request <https://github.com/django-nonrel/djangotoolbox/pull/56>`_ that adds Django 1.7 support to djangotoolbox. Until this pull request gets merged into the main djangotoolbox project you must install this library separaetly with the command:

    ``pip install git+https://github.com/kavdev/djangotoolbox.git@patch-1``

Datastax Cassandra-Driver
^^^^^^^^^^^^^^^^^^^^^^^^^

The python driver for Cassandra from Datastax is the leading way to interact with a Cassandra cluster. This used to be two seprate projects. The Cassandra driver and a wrapper called CqlEngine however now the two projects have merged into one. CqlEngine methods and objects are now found in the package cassandra.cqlengine.

Django
^^^^^^

As of this writing the only version I have tested this on is Django 1.7.7 and I believe there were some changes made to how the database backend layer adds filters to queries so it most likely only works on that version. It is a priority to add support for Django 1.8 as well as older, still suported versions of Django.
