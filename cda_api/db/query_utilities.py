from sqlalchemy import func, Integer, distinct
import sqlparse
from cda_api import get_logger, MappingError, ColumnNotFound, TableNotFound, SystemNotFound
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


# Gets statistics of a row for numeric columns
def numeric_summary(db, column):
    column_subquery = db.query(func.min(column).label('min'),
                 func.max(column).label('max'),
                 func.avg(column).label('mean'),
                 func.percentile_disc(0.5).within_group(column).label('median'),
                 func.percentile_disc(0.25).within_group(column).label('lower_quartile'),
                 func.percentile_disc(0.75).within_group(column).label('upper_quartile')).subquery('subquery')
    column_json = db.query(func.row_to_json(column_subquery.table_valued()).label(f'{column.name}_stats')).cte(f"json_{column.name}")
    return db.query(func.array_agg(get_cte_column(column_json, f"{column.name}_stats"))).scalar_subquery()


# Gets the categorical(grouped) json counts of a row
def categorical_summary(db, column):
    column_preselect = db.query(column, func.count(column).label('count_result')).group_by(column).subquery('subquery')
    column_json = db.query(func.row_to_json(column_preselect.table_valued()).label(f'{column.name}_categories')).cte(f"json_{column.name}")
    return db.query(func.array_agg(get_cte_column(column_json, f'{column.name}_categories'))).scalar_subquery()



# Combines the counts of data source columns into a single json for use in summary endpoint
def data_source_counts(db, data_source_columns):
    data_source_counts = [func.sum(func.cast(column, Integer)).label(column.name.split('_')[-1]) 
                          for column in data_source_columns]
    data_source_preselect = db.query(*data_source_counts).subquery('subquery')
    data_source_json = db.query(func.row_to_json(data_source_preselect.table_valued()))
    return data_source_json


# Gets the total count of an entity's related files and subjects by only counting from the mapping table (ie. observation_of_subject)
def entity_count(db, endpoint_tablename, preselect_query, entity_to_count):
    entity_relationship = DB_MAP.get_relationship(endpoint_tablename, entity_to_count)
    if entity_relationship.has_mapping_table:
        entity_local_column = entity_relationship.entity_column
        entity_mapping_column = entity_relationship.entity_mapping_column
        subquery = db.query(get_cte_column(preselect_query, entity_local_column.name).label(entity_mapping_column.name))
        entity_count_select = db.query(func.count(distinct(entity_relationship.foreign_mapping_column)).label('count_result')).filter(entity_mapping_column.in_(subquery)).scalar_subquery()
    else:
        entity_local_column = entity_relationship.entity_column
        subquery = db.query(get_cte_column(preselect_query, entity_local_column.name).label(entity_local_column.name))
        entity_count_select = db.query(func.count(distinct(entity_local_column)).label('count_result')).filter(entity_local_column.in_(subquery)).scalar_subquery()
    return entity_count_select


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


def build_unique_value_query(db, column, system = None, countOpt = False, limit=None, offset=None):
    if countOpt:
        unique_values_query = db.query(column, func.count().label('value_count')).group_by(column).order_by(column)
    else:
        unique_values_query = db.query(distinct(column).label(column.name)).order_by(column)

    if system:
        try:
            data_system_column = DB_MAP.get_meta_column(f"{column.table.name}_data_at_{system.lower()}")
            unique_values_query = unique_values_query.filter(data_system_column.is_(True))
        except Exception as e:
            error = SystemNotFound(f'system: {system} - not found')
            log.exception(error)
            raise error

    if limit:
        unique_values_query = unique_values_query.limit(limit)
    if offset:
        unique_values_query = unique_values_query.offset(offset)

    unique_values_query = unique_values_query.subquery('column_json')

    query = db.query(func.row_to_json(unique_values_query.table_valued()))
    return query
