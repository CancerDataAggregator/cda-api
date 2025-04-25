import ast
import re

from sqlalchemy.sql import exists, select

from cda_api import ParsingError, RelationshipNotFound
from cda_api.db import DB_MAP

from .query_operators import apply_filter_operator


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
        if split_filter_string[2] in ['in', 'like', 'not']:
            operator =  f'{operator} {split_filter_string[2]}'
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
        # Replace wildcards 
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

    return columnname, operator, value


# Generate preselect filter conditional
def get_preselect_filter(endpoint_tablename, filter_string, log):
    log.debug(f'Constructing filter "{filter_string}"')
    # get the components of the filter string
    filter_columnname, filter_operator, filter_value = parse_filter_string(filter_string, log)

    # ensure the unique column name exists in mapping and assign variables
    filter_column_info = DB_MAP.get_column_info(filter_columnname)

    filter_tablename = filter_column_info.tablename

    # build the sqlalachemy orm filter with the components
    filter_clause = apply_filter_operator(filter_column_info.metadata_column, filter_value, filter_operator, log)

    local_filter_clause = filter_clause
    # if the filter applies to a foreign table, preselect on the mapping column
    if filter_column_info.tablename.lower() != endpoint_tablename.lower():
        try:
            relationship = DB_MAP.get_relationship(
                entity_tablename=endpoint_tablename, foreign_tablename=filter_column_info.tablename
            )
            mapping_column = relationship.entity_collection
            filter_clause = mapping_column.any(filter_clause)
        except RelationshipNotFound:
            hanging_table_join = DB_MAP.get_hanging_table_join(
                hanging_tablename=filter_column_info.tablename, local_tablename=endpoint_tablename
            )
            if "entity_mapping_join" in hanging_table_join.keys():
                filter_clause = exists(
                    select(1)
                    .select_from(hanging_table_join["join_table"])
                    .filter(hanging_table_join["statement"])
                    .filter(hanging_table_join["mapping_table_join_clause"])
                    .filter(filter_clause)
                )
                pass
            else:
                if filter_column_info.tablename == 'upstream_identifiers':
                    upstream_identifiers_cda_table = DB_MAP.get_column_info('upstream_identifiers_cda_table')
                    filter_clause = exists(
                        select(1)
                        .select_from(hanging_table_join["join_table"])
                        .filter(hanging_table_join["statement"])
                        .filter(filter_clause)
                        .filter(upstream_identifiers_cda_table.metadata_column == endpoint_tablename)
                    )
                else:
                    filter_clause = exists(
                        select(1)
                        .select_from(hanging_table_join["join_table"])
                        .filter(hanging_table_join["statement"])
                        .filter(filter_clause)
                    )

        except Exception as e:
            raise e

    return filter_clause, local_filter_clause, filter_columnname, filter_tablename


# Build match_all and match_some filter conditional lists
def build_match_conditons(endpoint_tablename, request_body, log):
    log.info("Building MATCH conditions")
    match_all_conditions = []
    match_some_conditions = []
    filter_columnnames = []
    filter_table_map = {}
    # match_all_conditions will be all AND'd together
    if request_body.MATCH_ALL:
        for filter_string in request_body.MATCH_ALL:
            filter_clause, local_filter_clause, filter_columnname, filter_tablename = get_preselect_filter(endpoint_tablename, filter_string, log) 
            match_all_conditions.append(filter_clause)
            if filter_columnname not in filter_columnnames:
                filter_columnnames.append(filter_columnname)
            if filter_tablename in filter_table_map.keys():
                filter_table_map[filter_tablename]['match_all'].append(local_filter_clause)
            else:
                filter_table_map[filter_tablename] = {'match_all': [local_filter_clause], 'match_some': []}
    # match_some_conditions will be all OR'd together
    if request_body.MATCH_SOME:
        for filter_string in request_body.MATCH_SOME:
            filter_clause, local_filter_clause, filter_columnname, filter_tablename = get_preselect_filter(endpoint_tablename, filter_string, log) 
            match_some_conditions.append(filter_clause)
            if filter_columnname not in filter_columnnames:
                filter_columnnames.append(filter_columnname)
            if filter_tablename in filter_table_map.keys():
                filter_table_map[filter_tablename]['match_some'].append(local_filter_clause)
            else:
                filter_table_map[filter_tablename] = {'match_all': [], 'match_some': [local_filter_clause]}
    return match_all_conditions, match_some_conditions, filter_columnnames, filter_table_map
