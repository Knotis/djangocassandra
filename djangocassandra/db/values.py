from collections import OrderedDict


class PrimaryKeyValue(OrderedDict):
    def __int__(self):
        return self.__hash__()

    def __hash__(self):
        return hash(self.to_tuple())

    def __str__(self):
        return str(self.to_tuple())

    def __unicode__(self):
        return unicode(self.to_tuple())

    def to_tuple(self):
        return tuple(self.iteritems())

