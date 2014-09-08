Cassandra support for the Django web framework
==============================================

Install Apache Cassandra
-------
```
follow directions on this link: http://wiki.apache.org/cassandra/DebianPackaging
```

Testing
-------
```
virtualenv venv
source venv/bin/activate
pip install --upgrade ./
pip install nose 
pip install git+https://github.com/django-nonrel/django.git@nonrel-1.6
export DJANGO_SETTINGS_MODULE=tests.settings
nosetests
```
