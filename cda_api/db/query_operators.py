from sqlalchemy import func, Column
from cda_api import get_logger

log = get_logger()

def apply_filter_operator(filter_column, filter_value, filter_operator):
    match filter_operator.lower():
        case 'like':
            return case_insensitive_like(filter_column, filter_value)
        case 'notlike':
            return case_insensitive_not_like(filter_column, filter_value)
        case 'in':
            return in_array(filter_column, filter_value)
        case 'notin':
            return not_in_array(filter_column, filter_value)
        case '=':
            return filter_column == filter_value
        case '<':
            return filter_column < filter_value
        case '<=':
            return filter_column <= filter_value
        case '>':
            return filter_column > filter_value
        case '>=':
            return filter_column >= filter_value
        case '!=':
            return filter_column != filter_value
        case _:
            log.exception(f'Unexpected operator: {filter_operator}')
            raise ValueError

# Returns a case insensitive like filter conditional object
def case_insensitive_like(column, value):
    return func.coalesce(func.upper(column), '').like(func.upper(value))


# Returns a case insensitive like filter conditional object
def case_insensitive_not_like(column, value):
    return func.coalesce(func.upper(column), '').not_like(func.upper(value))

def in_array(column, value):
    if not isinstance(value, list):
        try:
            # TODO - better implementation needed
            value_list = eval(value)
            if not isinstance(value_list, list):
                log.exception(f'Expected list: {value}')
                raise TypeError
        except Exception as e:
            log.exception(f'Expected list: {value}\n{e}')
            raise
        return column.in_(value_list)
    else: 
        return column.in_(value)

def not_in_array(column, value):
    if not isinstance(value, list):
        try:
            # TODO - better implementation needed
            value_list = eval(value)
            if not isinstance(value_list, list):
                log.exception(f'Expected list: {value}')
                raise TypeError
        except Exception as e:
            log.exception(f'Expected list: {value}\n{e}')
            raise
        return column.notin_(value_list)
    else: 
        return column.notin_(value)
