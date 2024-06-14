from sqlalchemy import func, Integer, distinct
import sqlparse
from cda_api import get_logger, MappingError, ColumnNotFound, TableNotFound
from .schema import get_db_map

log = get_logger()
DB_MAP = get_db_map()

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


# Returns the column object from a cte (Common Table Expression) object
def get_cte_column(cte, columnname):
    return getattr(cte.c, columnname)


# distinct(count COLUMN)
def distinct_count(column):
    return func.count(distinct(column))

# Gets the total distinct counts of a column as a subquery
def total_count(db, column):
    return db.query(distinct_count(column).label('count_result')).scalar_subquery()


# Gets the categorical(grouped) json counts of a row
def grouped_count(db, column):
    column_preselect = db.query(column, distinct_count(column).label('count_result')).group_by(column).subquery('subquery')
    column_json = db.query(func.row_to_json(column_preselect.table_valued())).cte(f"json_{column.name}")
    return db.query(func.array_agg(column_json.table_valued())).scalar_subquery()


# Combines the counts of data source columns into a single json for use in summary endpoint
def data_source_counts(db, data_source_columns):
    data_source_counts = [func.sum(func.cast(column, Integer)).label(column.name.split('_')[-1]) 
                          for column in data_source_columns]
    data_source_preselect = db.query(*data_source_counts).subquery('subquery')
    data_source_json = db.query(func.row_to_json(data_source_preselect.table_valued())) # .cte(f"json_data_sources")
    # return db.query(func.array_agg(data_source_json.table_valued())).scalar_subquery()
    return data_source_json


# Gets the total count of an entity's related files by only counting from the mapping table (ie. observation_of_subject)
def entity_file_count(db, endpoint_tablename, preselect_query):
    entity_file_relationship = DB_MAP.get_relationship(endpoint_tablename, 'file')
    entity_to_file_local_column = entity_file_relationship.entity_column
    entity_to_file_mapping_column = entity_file_relationship.entity_mapping_column
    file_entity_subquery = db.query(get_cte_column(preselect_query, entity_to_file_local_column.name).label(entity_to_file_mapping_column.name))
    file_count_select = db.query(func.count(distinct(entity_file_relationship.foreign_mapping_column)).label('count_result')).filter(entity_to_file_mapping_column.in_(file_entity_subquery)).scalar_subquery()
    return file_count_select


# Combines select columns, match conditions, and mapping columns into cohesive query
def build_match_query(db, select_columns, match_all_conditions=None, match_some_conditions=None, mapping_columns=None):
    # Add select columns
    query = db.query(*select_columns)

    #Add filters
    if match_all_conditions and match_some_conditions:
        query = query.filter(*match_all_conditions).filter(*match_some_conditions)
    elif match_all_conditions:
        query = query.filter(*match_all_conditions)
    elif match_some_conditions:
        query = query.filter(*match_some_conditions)

    # Add joins
    if mapping_columns:
        # Add joins individually since .join(*mapping_columns) doesn't work for some reason
        for mapping_column in mapping_columns:
            query = query.join(mapping_column)

    return query
