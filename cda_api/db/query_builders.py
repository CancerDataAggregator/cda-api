from .filter_builder import build_match_conditons
from .select_builder import build_fetch_rows_select_clause, build_summary_select_clause
from .query_utilities import query_to_string, build_match_query, build_unique_value_query, distinct_count, build_filter_preselect
from sqlalchemy import and_, or_, func
from cda_api import get_logger
from cda_api.db import get_db_map
from cda_api.db.schema import Base

log = get_logger()
DB_MAP = get_db_map()


def paged_query(db, endpoint_tablename, qnode, limit, offset):
    """Generates json formatted row data based on input query

    Args:
        db (Session): Database session object
        endpoint_tablename (str): Name of the endpoint table
        qnode (QNode): JSON input query
        limit (int): Offset for paged results
        offset (int): Offset for paged results.

    Returns:
        PagedResponseObj: 
        { 
            'result': [{'column': 'data'}], 
            'query_sql': 'SQL statement used to generate result',
            'total_row_count': 'total rows of data for query generated (not paged)',
            'next_url': 'URL to acquire next paged result'
        }
    """

    log.info('Building paged query')
    # Build filter conditionals, select columns, and mapping columns lists
    match_all_conditions, match_some_conditions = build_match_conditons(endpoint_tablename, qnode)

    preselect_query, endpoint_id_alias = build_filter_preselect(db, endpoint_tablename, match_all_conditions, match_some_conditions)

    select_columns, foreign_array_preselects, foreign_joins = build_fetch_rows_select_clause(db, endpoint_tablename, qnode, preselect_query)

    query = db.query(*select_columns)

    if foreign_joins:
        for foreign_join in foreign_joins:
            query = query.join(**foreign_join, isouter=True)
    else:
        query = query.filter(endpoint_id_alias.in_(preselect_query))
        
    subquery = query.subquery('json_result')
    query = db.query(func.row_to_json(subquery.table_valued()))
    
    log.debug(f'Query:\n{"-"*100}\n{query_to_string(query, indented = True)}\n{"-"*100}')
    
    result = query.offset(offset).limit(limit).all()
    
    result = [row for row, in result]

    ret = {
        'result': result,
        'query_sql': query_to_string(query),
        'total_row_count': query.count(),
        'next_url': ''
    }
    return ret


# TODO
def summary_query(db, endpoint_tablename, qnode):
    """Generates json formatted summary data based on input query

    Args:
        db (Session): Database session object
        endpoint_tablename (str): Name of the endpoint table
        qnode (QNode): JSON input query

    Returns:
        SummaryResponseObj: 
        {
            'result': [{'summary': 'data'}],
            'query_sql': 'SQL statement used to generate result'
        }
    """

    log.info('Building paged query')
    

    # Get summary count query
    select_clause = build_summary_select_clause(db, endpoint_tablename, qnode)
    # wrap everything in a subquery
    subquery = db.query(*select_clause).subquery('json_result')
    # Apply row_to_json function
    query = db.query(func.row_to_json(subquery.table_valued()).label('results'))
    

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    result = query.all()
    result = [row for row, in result]

    # Fake return for now
    ret = {
        'result': result,
        'query_sql': query_to_string(query)
    }
    return ret

# TODO
def columns_query(db):
    """Generates list of column info for entity tables.

    Args:
        db (Session): Database session object

    Returns:
        ColumnResponseObj: 
        {
            'result': [{'key': 'value'}]
        }
    """

    cols = []
    
    #Get tablenames...
    tablenames = DB_MAP.entity_tables.keys()

    for tablename in tablenames:
        #Get columns for this table...
        columns = DB_MAP.get_table_column_infos(tablename)
        for column_info in columns:
            column = column_info.metadata_column
            if column.name != 'id_alias':
                col = dict()
                col['table'] = column_info.tablename
                col['column'] = column_info.uniquename
                col['data_type'] = str(column.type).lower()
                col['nullable'] = column.nullable
                col['description'] = 'unset'
                cols.append(col)
    
    ret = {
        'result': cols
    }

    return ret


# TODO
def unique_value_query(db, columnname, system, countOpt, totalCount, limit, offset):
    """Generates json formatted frequency results based on query for specific column

    Args:
        db (Session): Database session object
        TODO

    Returns:
        FrequencyResponseObj: 
        {
            'result': [{'frequency': 'data'}],
            'query_sql': 'SQL statement used to generate result'
        }
    """
    column = DB_MAP.get_meta_column(columnname)

    query, total_count_query = build_unique_value_query(db=db, 
                                     column=column, 
                                     system=system,
                                     countOpt=countOpt)
    
    result = query.offset(offset).limit(limit).all()
    result = [row for row, in result]
    total_count = total_count_query.scalar()

    # Fake return for now
    ret = {
        'result': result,
        'query_sql': query_to_string(query),
        'total_row_count': total_count,
        'next_url': ''
    }
    return ret



def release_metadata_query(db):
    query = db.query(Base.metadata.tables['release_metadata'])
    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')
    # Fake return for now
    ret = {
        'result': [{'release_metadata': 'success'}],
        'query_sql': query_to_string(query),
        'total_row_count': 0,
        'next_url': ''
    }
    return ret