from sqlalchemy import func
from .schema import Base, COLUMN_MAP
import sqlparse
from cda_api import get_logger, MappingError, ColumnNotFound, TableNotFound

log = get_logger()

# Generates compiled SQL string from query object
def query_to_string(q, indented=False) -> str:
    sql_string = str(q.statement.compile(compile_kwargs={"literal_binds": True}))
    if indented:
        return sqlparse.format(sql_string, reindent=True, keyword_case='upper')
    else:
        return sql_string.replace('\n', '')


# Prints compiled SQL string from query object
def print_query(q) -> None:
    print(query_to_string(q, indented=True))


# Function to get the mapping (collection) name to a foreign table
# Matches the cda_api.db.name_for_collection_relationship structure
def get_mapping_columnname(foreign_tablename, entity_tablename):
    return f'{foreign_tablename}_{entity_tablename}_alias_collection'

# Get the mapping column object of the entity table that maps to a foreign table
def get_mapping_column(foreign_tablename, entity_tablename):
    entity_table = get_table_by_name(entity_tablename)
    try:
        mapping_columnname = get_mapping_columnname(foreign_tablename, entity_tablename)
        mapping_column = getattr(entity_table, mapping_columnname)
        return mapping_column
    except Exception as e:
        log.exception(f'No mapping found between {entity_tablename} and {foreign_tablename}\n{e}')
        raise MappingError
    

# Returns the automapped base class object for the table
def get_table_by_name(tablename):
    for name, table in Base.classes.items():
        if name.lower() == tablename.lower():
            return table
    log.exception(f'Table Not Found: {tablename}')
    raise TableNotFound


# Checks if a columnname exists within the given generated cda_api.db.schema.COLUMN_MAP dictionary
def check_columnname_exists(columnname):
    if columnname not in COLUMN_MAP.keys():
        possible_cols = [k for k in COLUMN_MAP.keys() if k.endswith(columnname)]
        possible_cols.extend([k for k in COLUMN_MAP.keys() if k.startswith(columnname)])
        if possible_cols:
            log.exception(f'Column Not Found: {columnname}, did you mean: {possible_cols}')
        else:
            log.exception(f'Column Not Found: {columnname}')
        raise ColumnNotFound
