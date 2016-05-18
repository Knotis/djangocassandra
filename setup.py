from distutils.core import setup

setup(
    name='djangocassandra',
    version='0.5.0',
    description='Cassandra support for the Django web framework',
    long_description=(
        'The Cassandra database backend for Django has been '
        'developed to allow developers to utilize the Apache '
        'Cassandra Database in their Django projects. '
        'Read the full documentation at '
        'https://djangocassandra.readthedocs.org'
    ),
    author='Seth Denner',
    author_email='seth@knotis.com',
    maintainer='Knotis Inc.',
    maintainer_email='support@knotis.com',
    url='https://github.com/Knotis/djangocassandra',
    download_url=(
        'https://github.com/Knotis/djangocassandra/archive/master.zip'
    ),
    license='BSD License',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Django :: 1.7',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Internet',

    ],
    packages=[
        'djangocassandra',
        'djangocassandra.db',
        'djangocassandra.db.backends',
        'djangocassandra.db.backends.cassandra'
    ],
    install_requires=[
        'django>=1.7, < 1.8',
        'cassandra-driver==2.5.1',
        'blist',
        'djangotoolbox==1.7.0'
    ],
)
