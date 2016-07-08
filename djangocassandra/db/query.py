from django.db.models.query import QuerySet as DjangoQuerySet


class QuerySet(DjangoQuerySet):
    def next(self, limit=None):
        last_limit = len(self)

        if 0 == last_limit:
            return self.model.objects.none()

        last = self[last_limit - 1]

        new_set = self.model.objects.all()
        new_set.where = self.query.where
        new_set = new_set.filter(pk_token__gt=last.pk_token)

        if None is not limit:
            new_set = new_set[:limit]

        else:
            new_set = new_set[:last_limit]

        return new_set
