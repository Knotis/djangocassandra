try:
    import cassandra as Database

except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(
        "Error loading cassandra-driver module (python-driver): %s" % e
    )


class CassandraCursor(object):
    """
    A wrapper around Datastax Cassandra Python Driver Session class to
    make it behave like a database cursor in a relational database such as
    MySQL and so that we can catch particular exception instances and reraise
    them with the right types. Implemented as a wrapper, rather than a
    subclass, so that we aren't stuck to the particular underlying
    representation returned by Connection.cursor().
    """
    def __init__(self, session):
        self._reset_attributes()
        self.session = session

    def _reset_attributes(self):
        self.session = None
        self.rows = []
        self.index = -1
        self._last_statement = None
        self._with_rows = False
        self._lastrowid = None

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def with_rows(self):
        return self._with_rows

    @property
    def statement(self):
        return self._last_statement

    @property
    def column_name(self):
        raise NotImplementedError(
            'column_name propery has not been implemented'
        )

    @property
    def description(self):
        raise NotImplementedError(
            'description propery has not been implemented'
        )

    def fetchone(self):
        '''
        Note that this will have interesting behavior since there is
        no efficent way to check if we are at the last result, perhaps
        if the current result = first result?
        '''
        self.index += 1
        return self.rows[self.index - 1]

    def fetchmany(
        self,
        size=1
    ):
        many = self.rows[self.index:self.index + size]
        self.index += size

        return many

    def fetchall(self):
        remaining = self.rows[self.index:]
        self.rows = []
        self.index = -1

        return remaining

    def execute(self, query, args=None):
        self.rows = self.session.execute(query, args)
        self.index = 0
        self._with_rows = 0 != len(self.rows)

        return self.rows

    def executemany(self, query, args_list):
        prepared = self.session.prepare(query)
        batch = Database.BatchStatement()
        for args in args_list:
            batch.add(prepared, args)

        self.rows = self.session.execute(batch)
        self.index = 0
        self._with_rows = 0 != len(self.rows)

        return self.rows

    def rollback(self):
        pass

    def commit(self):
        pass

    def close(self):
        self._reset_attributes()

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.session, attr)

    def __iter__(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
