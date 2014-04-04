from distutils.core import setup

setup(
    name='djangocassandra',
    version='0.0.0',
    description='Cassandra support for the Django web framework',
    author='Seth Denner',
    author_email='seth@knotis.com',
    url='',
    packages=[
        'djangocassandra',
        'djangocassandra.db',
        'djangocassandra.db.backends',
        'djangocassandra.db.backends.cassandra'
    ],
    install_requires=[
        'django==1.6c1',
        'djangotoolbox>=1.6.2, < 1.7',
        'cassandra-driver>=1.0.2, < 2.0',
    ],
    dependency_links=[(
        'https://github.com/django-nonrel/django/tarball/1.6c1'
        '#egg=django-1.6c1'
    )]
)
