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
        'djangotoolbox>=1.6.2, < 1.7',
        'cassandra-driver>=1.0.2, < 2.0',
    ],
)
