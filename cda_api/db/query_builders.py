from .filter_builder import build_match_conditons
from .select_builder import build_select_clause
from .query_utilities import query_to_string
from sqlalchemy import and_, or_
from cda_api import get_logger

log = get_logger()


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

    # Construct select and filter clauses
    # TODO - do we need to cover cases without match_all?
    if match_some_conditions:
        q = db.query(*select_columns).filter(and_(*match_all_conditions)).filter(or_(*match_some_conditions))
    else:
        q = db.query(*select_columns).filter(and_(*match_all_conditions))

    # Add joins individually as .join(*mapping_columns) doesn't work for some reason
    for mapping_column in mapping_columns:
        q = q.join(mapping_column)

    log.debug(f'Query:\n{"-"*100}\n{query_to_string(q, indented = True)}\n{"-"*100}')
    
    # TODO - Currently no data but this will generate the actual result for paged queries
    # TODO - Need to figure out what to do when offset past available data
    # result = q.offset(offset).limit(limit).all()

    # Fake return for now
    ret = {
        'result': [{'paged_query': 'success'}],
        'query_sql': query_to_string(q),
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
    # Build filter conditionals, select columns, and mapping columns lists
    match_all_conditions, match_some_conditions = build_match_conditons(endpoint_tablename, qnode)
    select_columns, mapping_columns = build_select_clause(endpoint_tablename, qnode)

    # Construct select and filter clauses
    if match_some_conditions:
        q = db.query(*select_columns).filter(and_(*match_all_conditions)).filter(or_(*match_some_conditions))
    else:
        q = db.query(*select_columns).filter(and_(*match_all_conditions))

    # Add joins individually as .join(*mapping_columns) doesn't work for some reason
    for mapping_column in mapping_columns:
        q = q.join(mapping_column)

    # TODO - Need to figure out how to output summary query rather than query
    # log.debug(f'Query:\n{"-"*60}\n{query_to_string(q)}\n{"-"*60}')

    # TODO - Need to determine what info we need to include for the summary query (q.count() will only get the total row count)
    # result = q.count()

    # Fake return for now
    ret = {
        'result': [{'summary_query': 'success'}],
        'query_sql': 'SELECT summary_query FROM table'
    }
    return ret

# TODO
def columns_query(db):
    """Generates json formatted frequency results based on query for specific column

    Args:
        db (Session): Database session object

    Returns:
        ColumnResponseObj: 
        {
            'result': [{'frequency': 'data'}]
        }
    """

    # Fake return for now
    ret = {
        'result': [{'table': 'subject', 'column':'sex', 'data_type':'text', 'nullable': False, 'description':'boringdesc'}]
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