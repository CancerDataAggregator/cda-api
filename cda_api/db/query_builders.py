import time

from sqlalchemy import func

from cda_api import SystemNotFound
from cda_api.db import DB_INFO
from cda_api.db.schema import Base
from cda_api.classes.DataQuery import DataQuery
from cda_api.classes.SummaryQuery import SummaryQuery
from cda_api.classes.ColumnsQuery import ColumnsQuery
from cda_api.classes.ColumnValuesQuery import ColumnValuesQuery
from cda_api.classes.ReleaseMetadataQuery import ReleaseMetadataQuery

from .query_functions import (
    query_to_string,
)


def data_query(db, endpoint_table_name, request_body, limit, offset, log):
    """Generates json formatted row data based on input query

    Args:
        db (Session): Database session object
        endpoint_table_name (str): Name of the endpoint table
        request_body (request_body): JSON input query
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
    log.info("Building data query")

    data_query = DataQuery(db, DB_INFO, endpoint_table_name, request_body, log)
    log.debug(data_query)
    query = data_query.get_query()
    count_query = data_query.get_count_query()

    log.debug(f'Query:\n{"-"*100}\n{query_to_string(query)}\n{"-"*100}')
    log.debug(f'Count Query:\n{"-"*100}\n{query_to_string(count_query)}\n{"-"*100}')

    # Get results from the database
    log.info("Running the query")
    q_start_time = time.time()
    result = query.offset(offset).limit(limit).all()
    row_count = count_query.scalar()
    query_time = time.time() - q_start_time
    log.info(f"Query execution time: {query_time}s")

    # Format the results
    f_start_time = time.time()
    result = [row for (row,) in result] # [({column1: value},), ({column2: value},)] -> [{column1: value}, {column2: value}]
    format_time = time.time() - f_start_time
    log.info(f"Row formatting time: {format_time}s")
    log.info(f"Returning {len(result)} rows out of {row_count} results | limit={limit} & offset={offset}")

    ret = {"result": result, "query_sql": query_to_string(query), "total_row_count": row_count, "next_url": ""}
    return ret


# TODO
def summary_query(db, endpoint_table_name, request_body, log):
    """Generates json formatted summary data based on input query

    Args:
        db (Session): Database session object
        endpoint_tablename (str): Name of the endpoint table
        request_body (SummaryRequestBody): JSON input query

    Returns:
        SummaryResponseObj:
        {
            'result': [{'summary': 'data'}],
            'query_sql': 'SQL statement used to generate result'
        }
    """
    log.debug('Building summary query')
    summary_query = SummaryQuery(db, DB_INFO, endpoint_table_name, request_body, log)
    log.debug(summary_query)
    query = summary_query.get_query()

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    # Get results from the database
    log.info("Running the query")
    q_start_time = time.time()
    result = query.all()
    query_time = time.time() - q_start_time
    log.info(f"Query execution time: {query_time}s")

    # Format the results
    f_start_time = time.time()
    result = [row for (row,) in result] # [({column1: value},), ({column2: value},)] -> [{column1: value}, {column2: value}]
    format_time = time.time() - f_start_time
    log.info(f"Row formatting time: {format_time}s")


    # Fake return for now
    ret = {"result": result, "query_sql": query_to_string(query)}
    return ret


def columns_query(db, log):
    """Generates list of column info for entity tables.

    Args:
        db (Session): Database session object
        TODO

    Returns:
        ColumnResponseObj:
        {
            'result': [{'key': 'value'}]
        }
    """
    log.info('Building columns query')
    columns_query = ColumnsQuery(DB_INFO)

    return columns_query.get_result()


def column_values_query(db, column_name, data_source_string, limit, offset, log):
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
    log.info("Building column_values query")

    column_values_query = ColumnValuesQuery(db, DB_INFO, column_name, data_source_string, log)
    
    query = column_values_query.get_query()
    total_count_query = column_values_query.get_total_count_query()

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')
    log.debug(f'Total Count Query:\n{"-"*100}\n{query_to_string(total_count_query)}\n{"-"*100}')

    # Execute query
    start_time = time.time()
    result = query.offset(offset).limit(limit).all()
    result = [row for (row,) in result]

    # Execute total_count query
    total_count = total_count_query.scalar()

    query_time = time.time() - start_time
    log.info(f"Query execution time: {query_time}s")
    log.info(f"Returning {len(result)} rows out of {total_count} results | limit={limit} & offset={offset}")

    # Return the results
    ret = {"result": result, "query_sql": query_to_string(query), "total_row_count": total_count, "next_url": ""}
    return ret


def release_metadata_query(db, log):
    # Simply get all the rows in the release_metadata database
    log.info("Building release_metadata query")

    release_metadata_query = ReleaseMetadataQuery(db, DB_INFO)
    query = release_metadata_query.get_query()

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    result = query.all()
    result = [row for (row,) in result]
    log.info(f"Returning {len(result)} results")

    return {"result": result}
