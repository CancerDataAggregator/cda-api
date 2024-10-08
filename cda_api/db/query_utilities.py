from sqlalchemy import func, Integer, distinct, and_, or_
from sqlalchemy.dialects import postgresql
import sqlparse
from cda_api import get_logger, MappingError, ColumnNotFound, TableNotFound, SystemNotFound
from cda_api.db import DB_MAP

log = get_logger()

# Generates compiled SQL string from query object
def query_to_string(q, indented=False) -> str:
    sql_string = str(q.statement.compile(compile_kwargs={"literal_binds": True}, dialect=postgresql.dialect()))
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
def total_column_count_subquery(db, column):
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
    column_preselect = db.query(column, func.count().label('count_result')).group_by(column).subquery('subquery')
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
    entity_local_column = entity_relationship.entity_column
    entity_local_column_uniquename = DB_MAP.get_column_uniquename(entity_local_column.name, entity_local_column.table.name)
    if entity_relationship.has_mapping_table:
        entity_mapping_column = entity_relationship.entity_mapping_column
        subquery = db.query(get_cte_column(preselect_query, entity_local_column_uniquename).label(entity_local_column_uniquename))
        entity_count_select = db.query(func.count(distinct(entity_relationship.foreign_mapping_column)).label('count_result')).filter(entity_mapping_column.in_(subquery)).scalar_subquery()
    else:
        subquery = db.query(get_cte_column(preselect_query, entity_local_column_uniquename).label(entity_local_column_uniquename))
        entity_count_select = db.query(func.count(distinct(entity_local_column)).label('count_result')).filter(entity_local_column.in_(subquery)).scalar_subquery()
    return entity_count_select


def unique_column_array_agg(column):
    return func.array_remove(func.array_agg(distinct(column)), None).label(column.name)

def build_foreign_array_preselect(db, entity_tablename, foreign_tablename, columns, preselect_query):
    relation = DB_MAP.get_relationship(entity_tablename, foreign_tablename)
    if relation.has_mapping_table:
        select_cols = [unique_column_array_agg(column) for column in columns] + [relation.entity_mapping_column]
        foreign_array_preselect = db.query(
                                    *select_cols
                                ).filter(
                                    relation.entity_mapping_column.in_(preselect_query)
                                ).group_by(
                                    relation.entity_mapping_column
                                ).join(
                                    relation.mapping_table, relation.foreign_column == relation.foreign_mapping_column
                                ).cte(
                                    f'{foreign_tablename}_columns'
                                )
        target = foreign_array_preselect
        onclause = getattr(foreign_array_preselect.c, relation.entity_mapping_column.name) == relation.entity_column
        preselect_columns = [col for col in foreign_array_preselect.c if col.name != relation.entity_mapping_column.name]
        foreign_join = {'target': target, 'onclause': onclause}
    else:
        select_cols = [unique_column_array_agg(column) for column in columns] + [relation.foreign_column]
        foreign_array_preselect = db.query(
                                    *select_cols
                                ).filter(
                                    relation.foreign_column.in_(preselect_query)
                                ).group_by(
                                    relation.foreign_column
                                ).cte(
                                    f'{foreign_tablename}_columns'
                                )
        target = foreign_array_preselect
        onclause = getattr(foreign_array_preselect.c, relation.foreign_column.name) == relation.entity_column
        preselect_columns = [col for col in foreign_array_preselect.c if col.name != relation.foreign_column.name]
        foreign_join = {'target': target, 'onclause': onclause}
    return foreign_array_preselect, foreign_join, preselect_columns

def build_filter_preselect(db, endpoint_tablename, match_all_conditions, match_some_conditions):
    # Get the id_alias column
    endpoint_id_alias = DB_MAP.get_meta_column(f"{endpoint_tablename}_id_alias")

    # Set up the CTE preselect by selecting the id_alias column from it
    preselect_cte = db.query(endpoint_id_alias.label('id_alias'))

    # Apply filter conditionals
    if match_all_conditions and match_some_conditions:
        preselect_cte = preselect_cte.filter(and_(*match_all_conditions)).filter(or_(*match_some_conditions))
    elif match_all_conditions:
        preselect_cte = preselect_cte.filter(and_(*match_all_conditions))
    elif match_some_conditions:
        preselect_cte = preselect_cte.filter(or_(*match_some_conditions))
    

    preselect_cte = preselect_cte.cte(f'{endpoint_tablename}_preselect')
    preselect_query = db.query(preselect_cte.c.id_alias)
    return preselect_query, endpoint_id_alias


# Combines select columns, match conditions, and mapping columns into cohesive query
def build_match_query(db, select_columns, match_all_conditions=None, match_some_conditions=None, mapping_columns=None):
    # Add select columns
    query = db.query(*select_columns)

    #Add filters
    if match_all_conditions and match_some_conditions:
        query = query.filter(and_(*match_all_conditions)).filter(or_(*match_some_conditions))
    elif match_all_conditions:
        query = query.filter(and_(*match_all_conditions))
    elif match_some_conditions:
        query = query.filter(or_(*match_some_conditions))

    # Add joins
    if mapping_columns:
        # Add joins individually since .join(*mapping_columns) doesn't work for some reason
        for mapping_column in mapping_columns:
            query = query.join(mapping_column)

    return query
