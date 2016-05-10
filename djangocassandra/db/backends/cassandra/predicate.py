#   Copyright 2010 BSN, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import re
import itertools
import warnings

from cassandra.cqlengine.functions import Token

from cassandra import ConsistencyLevel
from cassandra.query import (
    SimpleStatement,
    ordered_dict_factory
)

from .exceptions import InefficientQueryError

from .utils import sort_rows

SECONDARY_INDEX_SUPPORT_ENABLED = True


class InvalidSortSpecException(Exception):
    def __init__(self):
        super(InvalidSortSpecException, self).__init__(
            'The row sort spec must be a sort spec '
            'tuple/list or a tuple/list of sort specs'
        )


class InvalidRowCombinationOpException(Exception):
    def __init__(self):
        super(InvalidRowCombinationOpException, self).__init__(
            'Invalid row combination operation'
        )


class InvalidPredicateOpException(Exception):
    def __init__(self):
        super(InvalidPredicateOpException, self).__init__(
            'Invalid/unsupported query predicate operation'
        )


COMPOUND_OP_AND = 1
COMPOUND_OP_OR = 2


class RangePredicate(object):
    def __init__(
        self,
        column,
        start=None,
        start_inclusive=True,
        end=None,
        end_inclusive=True
    ):
        self.column = column
        self.start = start
        self.start_inclusive = start_inclusive
        self.end = end
        self.end_inclusive = end_inclusive

    def __repr__(self):
        s = '(RANGE: '
        if self.start:
            op = '<=' if self.start_inclusive else '<'
            s += (unicode(self.start) + op)
        s += self.column
        if self.end:
            op = '>=' if self.end_inclusive else '>'
            s += (op + unicode(self.end))
        s += ')'
        return s

    def _is_exact(self):
        return (
            (self.start is not None) and
            (self.start == self.end) and
            self.start_inclusive and
            self.end_inclusive
        )

    def can_evaluate_efficiently(
        self,
        partition_columns,
        clustering_columns,
        indexed_columns
    ):
        if self._is_exact():
            return self.column in itertools.chain(
                ['pk__token'],
                partition_columns,
                clustering_columns,
                indexed_columns
            )

        else:
            return self.column in itertools.chain(
                ['pk__token'],
                clustering_columns
            )

    def incorporate_range_op(self, column, op, value, parent_compound_op):
        if column != self.column:
            return False

        # FIXME: The following logic could probably be tightened up a bit
        # (although perhaps at the expense of clarity?)
        if parent_compound_op == COMPOUND_OP_AND:
            if op == 'gt':
                if self.start is None or value >= self.start:
                    self.start = value
                    self.start_inclusive = False
                    return True
            elif op == 'gte':
                if self.start is None or value > self.start:
                    self.start = value
                    self.start_inclusive = True
                    return True
            elif op == 'lt':
                if self.end is None or value <= self.end:
                    self.end = value
                    self.end_inclusive = False
                    return True
            elif op == 'lte':
                if self.end is None or value < self.end:
                    self.end = value
                    self.end_inclusive = True
                    return True
            elif op == 'exact' or op == 'eq':
                if self._matches_value(value):
                    self.start = self.end = value
                    self.start_inclusive = self.end_inclusive = True
                    return True
            elif op == 'startswith':
                # For the end value we increment the ordinal value of the
                # last character in the start value and make the end value
                # not inclusive
                end_value = value[:-1] + chr(ord(value[-1])+1)
                if (
                    ((self.start is None) or (value > self.start)) and
                    ((self.end is None) or (end_value <= self.end))
                ):
                    self.start = value
                    self.end = end_value
                    self.start_inclusive = True
                    self.end_inclusive = False
                    return True
            else:
                raise InvalidPredicateOpException()
        elif parent_compound_op == COMPOUND_OP_OR:
            if op == 'gt':
                if self.start is None or value < self.start:
                    self.start = value
                    self.start_inclusive = False
                    return True
            elif op == 'gte':
                if self.start is None or value <= self.start:
                    self.start = value
                    self.start_inclusive = True
                    return True
            elif op == 'lt':
                if self.end is None or value > self.end:
                    self.end = value
                    self.end_inclusive = False
                    return True
            elif op == 'lte':
                if self.end is None or value >= self.end:
                    self.end = value
                    self.end_inclusive = True
                    return True
            elif op == 'exact' or op == 'eq':
                if self._matches_value(value):
                    return True
            elif op == 'startswith':
                # For the end value we increment the ordinal value of the
                # last character in the start value and make the end value
                # not inclusive
                end_value = value[:-1] + chr(ord(value[-1])+1)
                if (
                    ((self.start is None) or (value <= self.start)) and
                    ((self.end is None) or (end_value > self.end))
                ):
                    self.start = value
                    self.end = end_value
                    self.start_inclusive = True
                    self.end_inclusive = False
                    return True
        else:
            raise InvalidPredicateOpException()

        return False

    def _matches_value(self, value):
        if value is None:
            return False
        if self.start is not None:
            if self.start_inclusive:
                if value < self.start:
                    return False
            elif value <= self.start:
                return False

        if self.end is not None:
            if self.end_inclusive:
                if value > self.end:
                    return False
            elif value >= self.end:
                return False
        return True

    def row_matches(self, row):
        value = row.get(self.column, None)
        return self._matches_value(value)

    def get_matching_rows(self, query):
        rows = query.get_row_range(self)
        return rows


class OperationPredicate(object):
    def __init__(self, column, op, value=None):
        self.column = column
        self.op = op
        self.value = value
        if op == 'regex' or op == 'iregex':
            flags = re.I if op == 'iregex' else 0
            self.pattern = re.compile(value, flags)

    def __repr__(self):
        return '(OP: ' + self.op + ':' + unicode(self.value) + ')'

    def can_evaluate_efficiently(
        self,
        pk_column,
        clustering_columns,
        indexed_columns
    ):
        return False

    def row_matches(self, row):
        row_value = row.get(self.column, None)
        if self.op == 'isnull':
            return row_value is None
        # FIXME: Not sure if the following test is correct in all cases
        if (row_value is None) or (self.value is None):
            return False
        if self.op == 'in':
            return row_value in self.value
        if self.op == 'istartswith':
            return row_value.lower().startswith(self.value.lower())
        elif self.op == 'endswith':
            return row_value.endswith(self.value)
        elif self.op == 'iendswith':
            return row_value.lower().endswith(self.value.lower())
        elif self.op == 'iexact':
            return row_value.lower() == self.value.lower()
        elif self.op == 'contains':
            return row_value.find(self.value) >= 0
        elif self.op == 'icontains':
            return row_value.lower().find(self.value.lower()) >= 0
        elif self.op == 'regex' or self.op == 'iregex':
            return self.pattern.match(row_value) is not None
        else:
            raise InvalidPredicateOpException()

    def incorporate_range_op(self, column, op, value, parent_compound_op):
        return False

    def get_matching_rows(self, query):
        # get_matching_rows should only be called for predicates that can
        # be evaluated efficiently, which is not the case for
        # OperationPredicate's
        raise NotImplementedError(
            'get_matching_rows() called for inefficient predicate'
        )


class CompoundPredicate(object):
    def __init__(self, op, negated=False, children=None):
        self.op = op
        self.negated = negated
        self.children = children
        if self.children is None:
            self.children = []

    def __repr__(self):
        s = '('
        if self.negated:
            s += 'NOT '
        s += ('AND' if self.op == COMPOUND_OP_AND else 'OR')
        s += ': '
        first_time = True
        if self.children:
            for child_predicate in self.children:
                if first_time:
                    first_time = False
                else:
                    s += ','
                s += unicode(child_predicate)
        s += ')'
        return s

    def can_evaluate_efficiently(
        self,
        pk_column,
        clustering_columns,
        indexed_columns
    ):
        if self.negated:
            return False

        if self.op == COMPOUND_OP_AND:
            for child in self.children:
                if not child.can_evaluate_efficiently(
                    pk_column,
                    clustering_columns,
                    indexed_columns
                ):
                    return False

            return True

        elif self.op == COMPOUND_OP_OR:
            for child in self.children:
                if not child.can_evaluate_efficiently(
                        pk_column,
                        clustering_columns,
                        indexed_columns
                ):
                    return False

            return True

        else:
            raise InvalidPredicateOpException()

    def row_matches_subset(self, row, subset):
        if self.op == COMPOUND_OP_AND:
            matches = True
            for predicate in subset:
                if not predicate.row_matches(row):
                    matches = False
                    break

        elif self.op == COMPOUND_OP_OR:
            matches = False
            for predicate in subset:
                if predicate.row_matches(row):
                    matches = True
                    break

        else:
            raise InvalidPredicateOpException()

        if self.negated:
            matches = not matches

        return matches

    def row_matches(self, row):
        return self.row_matches_subset(row, self.children)

    def incorporate_range_op(self, column, op, value, parent_predicate):
        return False

    def add_filter(self, column, op, value):
        column_name = (
            column.db_column
            if column.db_column
            else column.column
        )
        if op in ('lt', 'lte', 'gt', 'gte', 'eq', 'exact', 'startswith'):
            if not len(self.children):
                child = RangePredicate(column_name)
                incorporated = child.incorporate_range_op(
                    column_name,
                    op,
                    value,
                    COMPOUND_OP_AND
                )
                assert incorporated
                self.children.append(child)

            else:
                incorporated = None
                for child in self.children:
                    incorporated = child.incorporate_range_op(
                        column_name,
                        op,
                        value,
                        self.op
                    )
                    if incorporated:
                        break

                if not incorporated:
                    child = RangePredicate(column_name)
                    incorporated = child.incorporate_range_op(
                        column_name,
                        op,
                        value,
                        COMPOUND_OP_AND
                    )
                    assert incorporated
                    self.children.append(child)
        else:
            child = OperationPredicate(column_name, op, value)
            self.children.append(child)

    def add_child(self, child_query_node):
        self.children.append(child_query_node)

    def get_matching_rows(self, query):
        pk_column = query.query.get_meta().pk.column

        # In the first pass we handle the query nodes that can be processed
        # efficiently. Hopefully, in most cases, this will result in a
        # subset of the rows that is much smaller than the overall number
        # of rows so we only have to run the inefficient query predicates
        # over this smaller number of rows.
        range_predicates = []
        inefficient_predicates = []
        result = None
        for predicate in self.children:
            if predicate.can_evaluate_efficiently(
                pk_column,
                query.clustering_columns,
                query.filterable_columns
            ):
                range_predicates.append(predicate)

            else:
                inefficient_predicates.append(predicate)

        cql_query = query.get_row_range(range_predicates)

        if query.ordering:
            for order in query.ordering:
                cql_query = cql_query.order_by(order)

        result = None

        def paged_query_generator(
            cql_query,
            django_query
        ):
            statement = SimpleStatement(
                str(cql_query._select_query()),
                consistency_level=ConsistencyLevel.ONE
            )

            if (
                hasattr(
                    django_query,
                    'cassandra_meta'
                ) and None is not django_query.cassandra_meta and
                hasattr(
                    django_query.cassandra_meta,
                    'fetch_size'
                )
            ):
                statement.fetch_size = django_query.cassandra_meta.fetch_size

            parameters = {}
            for where in cql_query._where:
                if isinstance(where.value, Token):
                    value = ''
                    for v in where.value.value:
                        value += v
                else:
                    value = where.value

                parameters[
                    str(where.query_value.context_id)
                ] = value

            django_query.connection.session.row_factory = (
                ordered_dict_factory
            )

            results = django_query.connection.session.execute(
                statement,
                parameters
            )

            for row in results:
                yield row

        result = paged_query_generator(
            cql_query,
            query
        )

        if (
            inefficient_predicates or
            query.inefficient_ordering
        ):
            if not query.allows_inefficient:
                raise InefficientQueryError(query)

            warnings.warn(InefficientQueryError.message)

        if inefficient_predicates:
            result = [
                row for row in result if self.row_matches_subset(
                    row,
                    inefficient_predicates
                )
            ]

        if query.inefficient_ordering:
            for order in query.inefficient_ordering:
                sort_rows(result, order)

        if query.low_mark is not None or query.high_mark is not None:
            from itertools import islice
            result = islice(result, query.low_mark, query.high_mark)

        return result
