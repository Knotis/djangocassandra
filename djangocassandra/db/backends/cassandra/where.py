from django.db.models.sql.where import (
    WhereNode,
    EverythingNode
)


class CQLWhereNode(WhereNode):
    def as_cql(
        self,
        qn,
        connection
    ):
        return self.as_sql(
            qn,
            connection
        )


class CQLEverythingNode(EverythingNode):
    pass
