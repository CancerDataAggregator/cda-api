from .filter_builder import build_match_conditons
from .select_builder import build_fetch_rows_select_clause
from .query_utilities import query_to_string, build_match_query, build_filter_preselect, total_column_count_subquery
from .query_utilities import entity_count, get_cte_column, numeric_summary, categorical_summary, data_source_counts
from sqlalchemy import func, distinct
from cda_api import get_logger, SystemNotFound
from cda_api.db import DB_MAP
from cda_api.db.schema import Base
import time


def fetch_rows(db, endpoint_tablename, qnode, limit, offset, log):
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
    log.info('Building fetch_rows query')

    # Build filter conditionals
    match_all_conditions, match_some_conditions = build_match_conditons(endpoint_tablename, qnode, log)

    # Build the preselect query 
    filter_preselect_query, endpoint_id_alias = build_filter_preselect(db, endpoint_tablename, match_all_conditions, match_some_conditions)

    # Build the select columns and joins to foreign column array preselects
    select_columns, foreign_array_preselects, foreign_joins = build_fetch_rows_select_clause(db, endpoint_tablename, qnode, filter_preselect_query, log)

    # Add select columns
    query = db.query(*select_columns)

    # Apply filterpreselect
    query = query.filter(endpoint_id_alias.in_(filter_preselect_query))

    # Optimize Count query by only counting the id_alias column based on the preselect filter
    count_subquery = db.query(endpoint_id_alias).filter(endpoint_id_alias.in_(filter_preselect_query)).subquery('rows_to_count')
    count_query = db.query(func.count()).select_from(count_subquery)

    # Add joins to foreign table preselects
    if foreign_joins:
        for foreign_join in foreign_joins:
            query = query.join(**foreign_join, isouter=True)
    
    # Convert to json format
    subquery = query.subquery('json_result')
    query = db.query(func.row_to_json(subquery.table_valued()))
    
    log.debug(f'Query:\n{"-"*100}\n{query_to_string(query, indented = True)}\n{"-"*100}')

    log.debug(f'Count Query:\n{"-"*100}\n{query_to_string(count_query, indented = True)}\n{"-"*100}')

    # Get results from the database 
    start_time = time.time()
    result = query.offset(offset).limit(limit).all()
    row_count = count_query.scalar()

    # [({column1: value},), ({column2: value},)] -> [{column1: value}, {column2: value}]
    result = [row for row, in result]
    query_time = time.time() - start_time
    log.info(f'Query execution time: {query_time}s')
    log.info(f'Returning {len(result)} rows out of {row_count} results | limit={limit} & offset={offset}')

    ret = {
        'result': result,
        'query_sql': query_to_string(query),
        'total_row_count': row_count,
        'next_url': ''
    }
    return ret


# TODO
def summary_query(db, endpoint_tablename, qnode, log):
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

    log.info('Building summary query')
    
    # Build filter conditionals
    match_all_conditions, match_some_conditions = build_match_conditons(endpoint_tablename, qnode, log)
    
    # Build preselect query
    endpoint_columns = DB_MAP.get_uniquename_metadata_table_columns(endpoint_tablename)
    endpoint_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    preselect_query = build_match_query(db=db,
                                        select_columns=endpoint_columns, 
                                        match_all_conditions=match_all_conditions,
                                        match_some_conditions=match_some_conditions)
    preselect_query = preselect_query.cte('filter_preselect')

    # Create list for select clause
    summary_select_clause = []

    # Get total count query
    total_count = total_column_count_subquery(db, get_cte_column(preselect_query, f'{endpoint_tablename}_id_alias')).label('total_count')
    summary_select_clause.append(total_count)

    # Get file or subject count
    if endpoint_tablename != 'subject':
        entity_to_count = 'subject'
    else:
        entity_to_count = 'file'
    sub_file_count = entity_count(db=db,
                                  endpoint_tablename=endpoint_tablename, 
                                  preselect_query=preselect_query,
                                  entity_to_count=entity_to_count)
    summary_select_clause.append(sub_file_count.label(f'{entity_to_count}_count'))

    # Get categorical & numeric summaries
    ## Step through each column in the endpoint table
    for column_info in endpoint_column_infos:
        column_summary = None
        ## Get the preselect column
        preselect_column = get_cte_column(preselect_query, column_info.uniquename)
        ## If column is supposed to be displayed in summary but not a data_source column:
        if column_info.summary_display and column_info.process_before_display != 'data_source':
            match column_info.column_type:
                case 'numeric':
                    column_summary = numeric_summary(db, preselect_column)
                    summary_select_clause.append(column_summary.label(f'{column_info.uniquename}_summary'))
                case 'categorical':
                    column_summary = categorical_summary(db, preselect_column)
                    summary_select_clause.append(column_summary.label(f'{column_info.uniquename}_summary'))
                case _:
                    log.warning(f'Unexpectedly skipping {column_info.column_name} for summary - column_type: {column_info.column_type}')
                    pass

    # Get data_source counts
    table_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    ## Get unique names of columns that have process_before_display of 'data_source' in the column_metadata table
    data_source_columnnames = [column_info.uniquename for column_info in table_column_infos if column_info.process_before_display == 'data_source']
    ## Get preselect columns of the 'data_source' columns
    data_source_columns = [column for column in preselect_query.c if column.name in data_source_columnnames]
    ## Get the data source select query
    data_source_count_select = data_source_counts(db, data_source_columns)
    summary_select_clause.append(data_source_count_select.label('data_source'))
    
    # Wrap everything in a subquery
    subquery = db.query(*summary_select_clause).subquery('json_result')
    query = db.query(func.row_to_json(subquery.table_valued()).label('results'))
    

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    start_time = time.time()
    result = query.all()
    result = [row for row, in result]
    query_time = time.time() - start_time
    log.info(f'Query execution time: {query_time}s')

    # Fake return for now
    ret = {
        'result': result,
        'query_sql': query_to_string(query)
    }
    return ret


def columns_query(db):
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

    cols = []
    
    tablenames = DB_MAP.entity_tables.keys()

    # Step through columns in each table and use their ColumnInfo class to return required information
    for tablename in tablenames:
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


def unique_value_query(db, columnname, system, countOpt, totalCount, limit, offset, log):
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
    log.info('Building unique_values query')

    column = DB_MAP.get_meta_column(columnname)

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
    
    unique_values_query = unique_values_query.subquery('column_json')
    
    query = db.query(func.row_to_json(unique_values_query.table_valued()))
    total_count_query = db.query(func.count()).select_from(unique_values_query)
    
    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query, indented = True)}\n{"-"*60}')
    log.debug(f'Total Count Query:\n{"-"*100}\n{query_to_string(total_count_query, indented = True)}\n{"-"*100}')

    # Execute query
    start_time = time.time()
    result = query.offset(offset).limit(limit).all()
    result = [row for row, in result]
    
    # Execute total_count query
    total_count = total_count_query.scalar()

    query_time = time.time() - start_time
    log.info(f'Query execution time: {query_time}s')
    log.info(f'Returning {len(result)} rows out of {total_count} results | limit={limit} & offset={offset}')

    # Fake return for now
    ret = {
        'result': result,
        'query_sql': query_to_string(query),
        'total_row_count': total_count,
        'next_url': ''
    }
    return ret



def release_metadata_query(db, log):
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