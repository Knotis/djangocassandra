from functools import cmp_to_key

from django.db.utils import DatabaseError

from django.db.models.sql.where import (
    WhereNode,
    AND,
    OR
)


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


def sort_rows(
    rows,
    ordering
):
    if None is ordering:
        return rows

    if (type(ordering) != list) and (type(ordering) != tuple):
        ordering = (ordering,)

    rows.sort(
        key=cmp_to_key(lambda row1, row2: _compare_rows(
            row1,
            row2,
            ordering
        ))
    )


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
