from .query_operators import apply_filter_operator
from cda_api import get_logger, ParsingError
from cda_api.db import get_db_map

log = get_logger()
DB_MAP = get_db_map()

import re
import ast

# Parse out the key components from the filter string
def parse_filter_string(filter_string):
    # Clean up the filter
    filter_string = filter_string.strip()

    # Parse out the operator (Note: Order matters, you can't put = before <=)
    operator_pattern = r"(?:\snot\s|\s)(?:!=|<>|<=|>=|=|<|>|is|in|like|between|not)+(?:\snot\s|\s)"
    operator_rexp = re.compile(operator_pattern)
    parsed_operators = [op.strip() for op in operator_rexp.findall(filter_string.lower())]
    if len(parsed_operators) != 1:
        raise ParsingError(f'Unable to parse out operator in filter: "{filter_string}"')

    # Get the operator from the list of matches 
    operator = parsed_operators[0]

    # Verify the matched operator is valid
    valid_operators = ['!=','<>','<=','>=','=','<','>','is','in','like','between',
                    'not','is not','not in','not like','not between']
    if operator not in valid_operators:
        raise ParsingError(f'Parsed operator: "{operator}" not valid')

    # Ensure the operator isn't at the beginning or the end of the filter string
    operator_location = re.search(operator, filter_string)
    if operator_location.start() == 0:
        raise ParsingError(f'Missing column in filter before operator "{filter_string}"')

    if operator_location.end() == len(filter_string):
        raise ParsingError(f'Missing value after operator "{filter_string}"')

    # Set columnname value to the stripped string before the operator
    columnname = filter_string[:operator_location.start()].strip()

    # Check if the string before the operator wasn't just whitespace
    if len(columnname) < 1:
        raise ParsingError(f'Missing column in filter before operator "{filter_string}"')

    # Ensure there is no whitespace in the parsed columnname
    if re.search('\s', columnname):
        raise ParsingError(f'Invalid column "{columnname}" in filter: "{filter_string}"')

    # Set columnname value to the stripped string after the operator
    value_string = filter_string[operator_location.end():].strip()

    # Use ast.literal_eval() to safely evaluate the value
    try:
        value = ast.literal_eval(value_string)
    except Exception:
        # If there is an error, just handle as a string
        value = value_string

    # Need to ensure lists and the operators "in"/"not in" are only ever used together
    if isinstance(value, list) and (operator not in ['in', 'not in']):
        raise ParsingError(f'Operator must be "in" or "not in" when using a list value -> filter: {filter_string}')

    elif (not isinstance(value, list)) and (operator in ['in', 'not in']):
        raise ParsingError(f'Value: {value_string} must be a list (ex. [1,2,3] or ["a","b","c"]) when using "in" or "not in" operators -> filter: "{filter_string}"')
    
    log.debug(f'columnname: {columnname}, operator: {operator}, value: {value}, value type: {type(value)}')
    
    return columnname, operator, value


# Generate preselect filter conditional
def get_preselect_filter(endpoint_tablename, filter_string):
    log.debug(f'Constructing filter "{filter_string}"')
    # get the components of the filter string
    filter_columnname, filter_operator, filter_value = parse_filter_string(filter_string)

    # ensure the unique column name exists in mapping and assign variables
    filter_column_info = DB_MAP.get_column_info(filter_columnname)

    # build the sqlalachemy orm filter with the components
    filter_clause = apply_filter_operator(filter_column_info.metadata_column, filter_value, filter_operator)
    
    # if the filter applies to a foreign table, preselect on the mapping column
    if filter_column_info.tablename.lower() != endpoint_tablename.lower():
        relationship = DB_MAP.get_relationship(entity_tablename=endpoint_tablename, foreign_tablename=filter_column_info.tablename)
        mapping_column = relationship.entity_collection
        filter_clause = mapping_column.any(filter_clause)
    
    return filter_clause

# Build match_all and match_some filter conditional lists
def build_match_conditons(endpoint_tablename, qnode):
    log.info('Building MATCH conditions')
    match_all_conditions = []
    match_some_conditions = []
    if qnode.MATCH_ALL:
        match_all_conditions = [get_preselect_filter(endpoint_tablename, filter_string)
                                    for filter_string in qnode.MATCH_ALL]
    if qnode.MATCH_SOME:
        match_some_conditions = [get_preselect_filter(endpoint_tablename, filter_string)
                                    for filter_string in qnode.MATCH_SOME]
    return match_all_conditions, match_some_conditions
