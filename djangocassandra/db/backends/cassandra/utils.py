from functools import cmp_to_key


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
