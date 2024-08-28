from sqlalchemy import func, Column
from cda_api import get_logger

log = get_logger()

def apply_filter_operator(filter_column, filter_value, filter_operator, log):
    match filter_operator.lower():
        case 'like':
            return case_insensitive_like(filter_column, filter_value)
        case 'not like':
            return case_insensitive_not_like(filter_column, filter_value)
        case 'in':
            return in_array(filter_column, filter_value)
        case 'not in':
            return not_in_array(filter_column, filter_value)
        case '=':
            if isinstance(filter_value, str):
                return case_insensitive_equals(filter_column, filter_value)
            else:
                return filter_column == filter_value
        case '!=':
            if isinstance(filter_value, str):
                return case_insensitive_not_equals(filter_column, filter_value)
            else:
                return filter_column != filter_value
        case '<':
            return filter_column < filter_value
        case '<=':
            return filter_column <= filter_value
        case '>':
            return filter_column > filter_value
        case '>=':
            return filter_column >= filter_value
        case _:
            raise ValueError(f'Unexpected operator: {filter_operator}')

# Returns a case insensitive like filter conditional object
def case_insensitive_like(column, value):
    return func.coalesce(func.upper(column), '').like(func.upper(value))

# Returns a case insensitive equals filter conditional object
def case_insensitive_equals(column, value):
    return func.coalesce(func.upper(column), '') == func.upper(value)

# Returns a case insensitive like filter conditional object
def case_insensitive_not_like(column, value):
    return func.coalesce(func.upper(column), '').not_like(func.upper(value))

# Returns a case insensitive equals filter conditional object
def case_insensitive_not_equals(column, value):
    return func.coalesce(func.upper(column), '') == func.upper(value)

def in_array(column, value):
    return column.in_(value)

def not_in_array(column, value):
    return column.notin_(value)
