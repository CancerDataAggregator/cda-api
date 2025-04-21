from sqlalchemy import func


def apply_filter_operator(filter_column, filter_value, filter_operator, log):
    log.debug(f"Applying filter {filter_column} {filter_operator} {filter_value}")
    match filter_operator.lower():
        case "like":
            return case_insensitive_like(filter_column, filter_value)
        case "not like":
            return case_insensitive_not_like(filter_column, filter_value)
        case "in":
            return in_array(filter_column, filter_value)
        case "not in":
            return not_in_array(filter_column, filter_value)
        case "=":
            if isinstance(filter_value, str):
                return case_insensitive_equals(filter_column, filter_value)
            else:
                return filter_column == filter_value
        case "!=":
            if isinstance(filter_value, str):
                return case_insensitive_not_equals(filter_column, filter_value)
            else:
                return filter_column != filter_value
        case "<":
            return filter_column < filter_value
        case "<=":
            return filter_column <= filter_value
        case ">":
            return filter_column > filter_value
        case ">=":
            return filter_column >= filter_value
        case "is":
            if filter_value not in [None, True, False]:
                raise ValueError(
                    f"Operator '{filter_operator}' not compatible with value '{filter_value}'s type. Must use 'NULL', 'TRUE', or 'FALSE' for this operator."
                )
            return filter_column.is_(filter_value)
        case "is not":
            if filter_value not in [None, True, False]:
                raise ValueError(
                    f"Operator '{filter_operator}' not compatible with value '{filter_value}'s type. Must use 'NULL', 'TRUE', or 'FALSE' for this operator."
                )
            return filter_column.is_not(filter_value)
        case _:
            raise ValueError(f"Unexpected operator: {filter_operator}")


# Returns a case insensitive like filter conditional object
def case_insensitive_like(column, value):
    return func.coalesce(func.upper(column), "").like(func.upper(value))


# Returns a case insensitive equals filter conditional object
def case_insensitive_equals(column, value):
    return func.coalesce(func.upper(column), "") == func.upper(value)


# Returns a case insensitive like filter conditional object
def case_insensitive_not_like(column, value):
    return func.coalesce(func.upper(column), "").not_like(func.upper(value))


# Returns a case insensitive equals filter conditional object
def case_insensitive_not_equals(column, value):
    return func.coalesce(func.upper(column), "") == func.upper(value)


# Returns a case insensitive 'is not' filter conditional object
def case_insensitive_is_not(column, value):
    return func.coalesce(func.upper(column), "").is_not(func.upper(value))


def in_array(column, value):
    if isinstance(value[0], str):
        return func.coalesce(func.upper(column), "").in_([item.upper() for item in value]) 
    else:
        return column.in_(value)


def not_in_array(column, value):
    if isinstance(value[0], str):
        return func.coalesce(func.upper(column), "").not_in([item.upper() for item in value]) 
    else:
        return column.not_in(value)
