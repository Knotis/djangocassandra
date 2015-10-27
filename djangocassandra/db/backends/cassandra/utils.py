import sys

from functools import (
    wraps
)

from django.db.utils import DatabaseError

from django.db.models.sql.where import (
    WhereNode,
    AND,
    OR
)


def safe_call(func):
    @wraps(func)
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            raise DatabaseError, DatabaseError(*tuple(e)), sys.exc_info()[2]
    return _func


def _compare_rows(
    row1,
    row2,
    ordering
):
    for order in ordering:
        column_name = order[0]
        reverse = order[1] if len(order) > 1 else False
        row1_value = row1.get(column_name, None)
        row2_value = row2.get(column_name, None)
        result = cmp(row1_value, row2_value)
        if result != 0:
            if reverse:
                result = -result
            break

        else:
            result = 0

    return result


def _test_row(
    row,
    column,
    value,
    lookup_type='exact'
):
    if 'exact' is lookup_type:
        if row[column] != value:
            return False

    else:
        raise DatabaseError(
            'lookup_type %s not supported (yet).' % lookup_type
        )

    return True


def _row_matches(
    row,
    where
):
    result = True
    for child in where.children:
        if isinstance(child, WhereNode):
            matches = _row_matches(row, child)

        else:
            constraint, lookup_type, _, value = child
            matches = _test_row(
                row,
                constraint.col,
                value,
                lookup_type
            )

        if AND == where.connector:
            if not matches:
                result = False
                break

        elif OR == where.connector:
            result = result or matches

    if where.negated:
        return not result

    else:
        return result


def filter_rows(
    rows,
    where
):
    return [
        row for row in rows
        if _row_matches(
            row,
            where
        )
    ]


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


def _cmp_to_key(comparison_function):
    """
    Convert a cmp= function into a key= function.
    This is built in to Python 2.7, but we define it ourselves
    to work with older versions of Python
    """
    class K(object):
        def __init__(self, obj, *args):
            self.obj = obj
        def __lt__(self, other):
            return comparison_function(self.obj, other.obj) < 0
        def __gt__(self, other):
            return comparison_function(self.obj, other.obj) > 0
        def __eq__(self, other):
            return comparison_function(self.obj, other.obj) == 0
        def __le__(self, other):
            return comparison_function(self.obj, other.obj) <= 0
        def __ge__(self, other):
            return comparison_function(self.obj, other.obj) >= 0
        def __ne__(self, other):
            return comparison_function(self.obj, other.obj) != 0
    return K

def _compare_rows(row1, row2, sort_spec_list):
    for sort_spec in sort_spec_list:
        column_name = sort_spec[0]
        reverse = sort_spec[1] if len(sort_spec) > 1 else False
        row1_value = row1.get(column_name, None)
        row2_value = row2.get(column_name, None)
        result = cmp(row1_value, row2_value)
        if result != 0:
            if reverse:
                result = -result
            break;
    else:
        result = 0
    return result

def sort_rows(rows, sort_spec):
    if sort_spec == None:
        return rows

    if not isinstance(rows, list):
        rows = list(rows)

    if (type(sort_spec) != list) and (type(sort_spec) != tuple):
        raise InvalidSortSpecException()
    
    # The sort spec can be either a single sort spec tuple or a list/tuple
    # of sort spec tuple. To simplify the code below we convert the case
    # where it's a single sort spec tuple to a 1-element tuple containing
    # the sort spec tuple here.
    if (type(sort_spec[0]) == list) or (type(sort_spec[0]) == tuple):
        sort_spec_list = sort_spec
    else:
        sort_spec_list = (sort_spec,)
    
    rows.sort(key=_cmp_to_key(lambda row1, row2: _compare_rows(row1, row2, sort_spec_list)))

COMBINE_INTERSECTION = 1
COMBINE_UNION = 2

def combine_rows(rows1, rows2, op, primary_key_column):
    # Handle cases where rows1 and/or rows2 are None or empty
    if not rows1:
        return list(rows2) if rows2 and (op == COMBINE_UNION) else []
    if not rows2:
        return list(rows1) if (op == COMBINE_UNION) else []
    
    # We're going to iterate over the lists in parallel and
    # compare the elements so we need both lists to be sorted
    # Note that this means that the input arguments will be modified.
    # We could optionally clone the rows first, but then we'd incur
    # the overhead of the copy. For now, we'll just always sort
    # in place, and if it turns out to be a problem we can add the
    # option to copy
    sort_rows(rows1,(primary_key_column,))
    sort_rows(rows2,(primary_key_column,))
    
    combined_rows = []
    iter1 = iter(rows1)
    iter2 = iter(rows2)
    update1 = update2 = True
    
    while True:
        # Get the next element from one or both of the lists
        if update1:
            try:
                row1 = iter1.next()
            except:
                row1 = None
            value1 = row1.get(primary_key_column, None) if row1 != None else None
        if update2:
            try:
                row2 = iter2.next()
            except:
                row2 = None
            value2 = row2.get(primary_key_column, None) if row2 != None else None
        
        if (op == COMBINE_INTERSECTION):
            # If we've reached the end of either list and we're doing an intersection,
            # then we're done
            if (row1 == None) or (row2 == None):
                break
        
            if value1 == value2:
                combined_rows.append(row1)
        elif (op == COMBINE_UNION):
            if row1 == None:
                if row2 == None:
                    break;
                combined_rows.append(row2)
            elif (row2 == None) or (value1 <= value2):
                combined_rows.append(row1)
            else:
                combined_rows.append(row2)
        else:
            raise InvalidCombineRowsOpException()
        
        update1 = (row2 == None) or (value1 <= value2)
        update2 = (row1 == None) or (value2 <= value1)
    
    return combined_rows
