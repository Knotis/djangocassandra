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
        'django>=1.7, < 1.8',
        'djangotoolbox>=1.6.2, < 1.7',
        'cassandra-driver>=2.1.4',
    ],
)
