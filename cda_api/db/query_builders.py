from .filter_builder import build_match_conditons
from .select_builder import build_select_clause, build_summary_select_clause
from .query_utilities import query_to_string, build_match_query
from sqlalchemy import and_, or_, func
from cda_api import get_logger
from .schema import get_db_map

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
    select_columns, mapping_columns = build_select_clause(endpoint_tablename, qnode)

    query = build_match_query(db=db, 
                              select_columns=select_columns,
                              match_all_conditions=match_all_conditions,
                              match_some_conditions=match_some_conditions,
                              mapping_columns=mapping_columns)
    
    log.debug(f'Query:\n{"-"*100}\n{query_to_string(query, indented = True)}\n{"-"*100}')
    
    # TODO - Currently no data but this will generate the actual result for paged queries
    # TODO - Need to figure out what to do when offset past available data
    # result = q.offset(offset).limit(limit).all()

    # Fake return for now
    ret = {
        'result': [{'paged_query': 'success'}],
        'query_sql': query_to_string(query),
        'total_row_count': 42,
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
    select_clause = build_summary_select_clause(db, 'subject', qnode)
    # wrap everything in a subquery
    subquery = db.query(*select_clause).subquery('json_result')
    # Apply row_to_json function
    query = db.query(func.row_to_json(subquery.table_valued()).label('results'))

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    # TODO - This gets the result, but skipping for now while returning fake data
    # result = q.all()

    # Fake return for now
    ret = {
        'result': [{'summary_query': 'success'}],
        'query_sql': query_to_string(query)
    }
    return ret


# TODO
def frequency_query(db, columnname, qnode):
    """Generates json formatted frequency results based on query for specific column

    Args:
        db (Session): Database session object
        columnname (str): Input column name for frequency results
        qnode (QNode): JSON input query

    Returns:
        FrequencyResponseObj: 
        {
            'result': [{'frequency': 'data'}],
            'query_sql': 'SQL statement used to generate result'
        }
    """

    # Fake return for now
    ret = {
        'result': [{'frequency_query': 'success'}],
        'query_sql': 'SELECT frequency_query FROM table'
    }
    return ret



