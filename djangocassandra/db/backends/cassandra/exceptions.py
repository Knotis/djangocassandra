from django.db import (
    NotSupportedError
)


class InefficientQueryError(NotSupportedError):
    message = (
        'Innefficent queries are not allowed on this model. '
        'Make sure if you are filtering that you are only '
        'filtering on either primary key or indexed fields '
        'and that if you are ordering that you are only '
        'ordering on fields that are components of compound '
        'primary keys.\n\nAlternatively you can enable '
        'inefficient filtering and ordering in your database '
        'by setting the ALLOW_INEFFICIENT_QUERIES=True in '
        'your settings.py or you can set this per model in '
        'the Cassandra meta class for you model '
        '"allow_inefficient_queries=True".'
    )

    def __init__(self, query):
        self.query = query

    def __str__(self):
        return  (self.message + ':\n%s') % (repr(self.query),)
