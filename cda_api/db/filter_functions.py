import ast
from sqlalchemy import func
from cda_api import ParsingError


# Parse out the key components from the filter string
def parse_filter_string(filter_string, log):
    # Clean up the filter
    filter_string = filter_string.strip()
    split_filter_string = filter_string.split()
    if len(split_filter_string) < 3:
        raise ParsingError(f'Unable to parse out operator in filter: "{filter_string}"')
    columnname = split_filter_string[0]
    operator = split_filter_string[1]
    value_string = ' '.join(split_filter_string[2:])
    if len(split_filter_string) > 3:
        if split_filter_string[2].lower() in ['in', 'like', 'not']:
            operator =  f'{operator} {split_filter_string[2].lower()}'
            value_string = ' '.join(split_filter_string[3:])

    # Verify the matched operator is valid
    valid_operators = [
        "!=",
        "<>",
        "<=",
        ">=",
        "=",
        "<",
        ">",
        "is",
        "in",
        "like",
        "not",
        "is not",
        "not in",
        "not like",
    ]
    if operator.lower() not in valid_operators:
        raise ParsingError(f'Parsed operator: "{operator}" is not a valid operator')


    # Use ast.literal_eval() to safely evaluate the value
    try:
        value = ast.literal_eval(value_string)
    except Exception:
        # If there is an error, just handle as a string
        value = value_string

    # Check if value is null
    if isinstance(value, str):
        if value.lower() == "null":
            value = None
        elif value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        # Replace wildcards
        else:
            value = value.replace('*', '%')

    elif isinstance(value, set) or isinstance(value, tuple):
        value = list(value)

    # Throw error on dictionary filter
    elif isinstance(value, dict):
        raise ParsingError(f'Dictionary filters are not accepted: {filter_string}')

    # Need to ensure lists and the operators "in"/"not in" are only ever used together
    if isinstance(value, list) and (operator not in ["in", "not in"]):
        raise ParsingError(f'Operator must be "in" or "not in" when using a list value -> filter: {filter_string}')

    elif (not isinstance(value, list)) and (operator in ["in", "not in"]):
        raise ParsingError(
            f'Value: {value_string} must be a list (ex. [1,2,3] or ["a","b","c"]) when using "in" or "not in" operators -> filter: "{filter_string}"'
        )

    log.debug(f"columnname: {columnname}, operator: {operator}, value: {value}, value type: {type(value)}")

    return columnname.lower(), operator.lower(), value




def apply_filter_operator(filter_column, filter_value, filter_operator, log):
    log.debug(f"Building SQLAlchemy filter: {filter_column} {filter_operator} {filter_value}")
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
            if type(filter_value) not in [type(None), bool]:
                raise ParsingError(
                    f"Operator '{filter_operator}' not compatible with value '{filter_value}'s type. Must use 'NULL', 'TRUE', or 'FALSE' for this operator."
                )
            return filter_column.is_(filter_value)
        case "is not":
            if type(filter_value) not in [type(None), bool]:
                raise ParsingError(
                    f"Operator '{filter_operator}' not compatible with value '{filter_value}'s type. Must use 'NULL', 'TRUE', or 'FALSE' for this operator."
                )
            return filter_column.is_not(filter_value)
        case _:
            raise ParsingError(f"Unexpected operator: {filter_operator}")


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
