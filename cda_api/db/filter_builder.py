from .query_operators import apply_filter_operator
from cda_api import get_logger
from .schema import get_db_map

from cda_api.application_utilities import is_float, is_int

log = get_logger()
DB_MAP = get_db_map()

# Parse out the key components from the filter string
def parse_filter_string(filter_string):
    # TODO actually parse correctly
    filter_columnname, filter_operator, filter_value = filter_string.split()
    if is_int(filter_value):
        filter_value = int(filter_value)
    elif is_float(filter_value):
        filter_value = float(filter_value)
    return filter_columnname, filter_operator, filter_value



# Generate preselect filter conditional
def get_preselect_filter(endpoint_tablename, filter_string):
    log.debug(f'Constructing filter "{filter_string}"')
    # get the components of the filter string
    filter_columnname, filter_operator, filter_value = parse_filter_string(filter_string)

    # ensure the unique column name exists in mapping and assign variables
    filter_column_info = DB_MAP.get_column_info(filter_columnname)

    # build the sqlalachemy orm filter with the components
    # filter_clause = case_insensitive_like(filter_col['COLUMN'], filter_value)
    filter_clause = apply_filter_operator(filter_column_info.metadata_column, filter_value, filter_operator)
    
    # if the filter applies to a foreign table, preselect on the mapping column
    if filter_column_info.tablename.lower() != endpoint_tablename.lower():
        relationship = DB_MAP.get_relationship(entity_tablename=endpoint_tablename, foreign_tablename=filter_column_info.tablename)
        mapping_column = relationship.entity_collection
        # mapping_column = get_mapping_column(filter_column_info.tablename, endpoint_tablename)
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
