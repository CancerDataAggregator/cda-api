import time

from sqlalchemy import distinct, func, union_all, union, SelectLabelStyle

from cda_api import SystemNotFound, ColumnNotFound, InvalidFilterError
from cda_api.db import DB_MAP
from cda_api.db.schema import Base
from psycopg2.errors import UndefinedFunction

from .filter_builder import build_match_conditons
from .query_utilities import (
    normalize_add_exclude_columns,
    build_filter_preselect,
    get_foreign_array_summary_selects,
    build_match_query,
    categorical_summary,
    data_source_counts,
    entity_count,
    get_cte_column,
    numeric_summary,
    query_to_string,
    total_column_count_subquery,
    print_query
)
from .select_builder import build_data_select_clause


def data_query(db, endpoint_tablename, request_body, limit, offset, log):
    """Generates json formatted row data based on input query

    Args:
        db (Session): Database session object
        endpoint_tablename (str): Name of the endpoint table
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
    log.info("Building fetch_rows query")

    # Get match_all and match_some filters
    match_all_conditions, match_some_conditions, filter_columnnames, filter_table_map = build_match_conditons(endpoint_tablename, request_body, log)
    
    # normalize the add and exclude columns with the new filter columns as well as breaking out the table.* columns:
    request_body = normalize_add_exclude_columns(request_body, 'data', filter_columnnames, log)

    # Build the preselect query
    filter_preselect_query, endpoint_id_alias = build_filter_preselect(
        db, endpoint_tablename, match_all_conditions, match_some_conditions, log
    )

    # Build the select columns and joins to foreign column array preselects
    select_columns, foreign_joins = build_data_select_clause(
        db, endpoint_tablename, request_body, filter_preselect_query, filter_table_map, log
    )
    
    log.info(f"Constructing data query")
    query = db.query(*select_columns)
    query = query.filter(endpoint_id_alias.in_(filter_preselect_query))
    # Add joins to foreign table preselects
    if foreign_joins:
        log.info(f"Adding joins")
        for foreign_join in foreign_joins:
            query = query.join(**foreign_join, isouter=True)
    # query = add_hanging_table_joins(endpoint_tablename, select_columns, query)
    # Optimize Count query by only counting the id_alias column based on the preselect filter
    log.info(f"Constructing count query")
    count_subquery = (
        db.query(endpoint_id_alias).filter(endpoint_id_alias.in_(filter_preselect_query)).subquery("rows_to_count")
    )
    count_query = db.query(func.count()).select_from(count_subquery)

    subquery = query.subquery("json_result")
    log.info("Finalizing query format")
    query = db.query(func.row_to_json(subquery.table_valued()))

    log.debug(f'Query:\n{"-"*100}\n{query_to_string(query)}\n{"-"*100}')

    log.debug(f'Count Query:\n{"-"*100}\n{query_to_string(count_query)}\n{"-"*100}')

    # Get results from the database
    log.info("Running the query")
    q_start_time = time.time()
    result = query.offset(offset).limit(limit).all()
    row_count = count_query.scalar()
    query_time = time.time() - q_start_time
    log.info(f"Query execution time: {query_time}s")


    # [({column1: value},), ({column2: value},)] -> [{column1: value}, {column2: value}]
    f_start_time = time.time()
    result = [row for (row,) in result]
    format_time = time.time() - f_start_time
    log.info(f"Row formatting time: {format_time}s")
    log.info(f"Returning {len(result)} rows out of {row_count} results | limit={limit} & offset={offset}")

    ret = {"result": result, "query_sql": query_to_string(query), "total_row_count": row_count, "next_url": ""}
    return ret


# TODO
def summary_query(db, endpoint_tablename, request_body, log):
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

    log.info("Building summary query")

    # Build filter conditionals
    match_all_conditions, match_some_conditions, filter_columnnames, filter_table_map  = build_match_conditons(endpoint_tablename, request_body, log)

    # normalize the add and exclude columns with the new filter columns as well as breaking out the table.* columns:
    request_body = normalize_add_exclude_columns(request_body, 'summary', filter_columnnames, log)

    # Build preselect query
    endpoint_columns = DB_MAP.get_uniquename_metadata_table_columns(endpoint_tablename)
    endpoint_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    virtual_table_column_infos = DB_MAP.get_virtual_table_column_infos(endpoint_tablename)
    endpoint_column_infos.extend(virtual_table_column_infos)

    match_query = build_match_query(db=db,
                                    select_columns=endpoint_columns, 
                                    match_all_conditions=match_all_conditions,
                                    match_some_conditions=match_some_conditions)

    preselect_query = match_query.cte(f'{endpoint_tablename}_preselect')

    # Create list for select clause
    summary_select_clause = []

    # Get total count query
    total_count = total_column_count_subquery(db, preselect_query).label('total_count')
    # total_count = total_column_count_subquery(db, get_cte_column(preselect_query, f'{endpoint_tablename}_id_alias')).label('total_count')
    summary_select_clause.append(total_count)

    # Get file or subject count
    if endpoint_tablename != 'subject':
        entity_to_count = 'subject'
    else:
        entity_to_count = 'file'

    sub_file_count = entity_count(db=db,
                                endpoint_tablename=endpoint_tablename, 
                                preselect_query=preselect_query,
                                entity_to_count=entity_to_count,
                                filter_table_map=filter_table_map)
    
    summary_select_clause.append(sub_file_count.label(f'{entity_to_count}_count'))

    # Get categorical & numeric summaries
    ## Step through each column in the endpoint table
    for column_info in endpoint_column_infos:
        column_summary = None
        if not column_info.summary_returns:
            log.debug(f'Skipping column: {column_info.uniquename} because it is not supposed to be displayed')
            continue
        elif column_info.uniquename in request_body.EXCLUDE_COLUMNS:
            log.debug(f'Skipping column: {column_info.uniquename} because it is in the EXCLUDE_COLUMNS list')
            continue
        if column_info.virtual_table == None:
            ## Get the preselect column
            preselect_column = get_cte_column(preselect_query, column_info.uniquename)
            ## If column is supposed to be displayed in summary but not a data_source column:
            if column_info.process_before_display != 'data_source':  
                match column_info.column_type:
                    case 'numeric':
                        column_summary = numeric_summary(db, preselect_column)
                        summary_select_clause.append(column_summary.label(f'{column_info.uniquename}_summary'))
                    case 'categorical':
                        column_summary = categorical_summary(db, preselect_column)
                        summary_select_clause.append(column_summary.label(f'{column_info.uniquename}_summary'))
                    case _:
                        log.warning(f'Unexpectedly skipping {column_info.uniquename} for summary - column_type: {column_info.column_type}')
                        pass
        elif column_info.uniquename not in request_body.ADD_COLUMNS:
            add_columns_selects, _ = get_foreign_array_summary_selects(db, endpoint_tablename, [column_info.uniquename], preselect_query, filter_table_map, log)
            for select in add_columns_selects:
                summary_select_clause.append(db.query(select).label(select.name))

    # Get data_source counts
    table_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    ## Get unique names of columns that have process_before_display of 'data_source' in the column_metadata table
    data_source_columnnames = [column_info.uniquename for column_info in table_column_infos if column_info.process_before_display == 'data_source']
    ## Get preselect columns of the 'data_source' columns
    data_source_columns = [column for column in preselect_query.c if column.name in data_source_columnnames]
    ## Get the data source select query
    data_source_count_select = data_source_counts(db, data_source_columnnames, data_source_columns)
    summary_select_clause.append(data_source_count_select.label('data_source'))

    if request_body.ADD_COLUMNS != None:
        columns_to_add =  [columnname for columnname in request_body.ADD_COLUMNS if columnname not in request_body.EXCLUDE_COLUMNS]
        add_columns_selects, entity_total_count_select = get_foreign_array_summary_selects(db, endpoint_tablename, columns_to_add, preselect_query, filter_table_map, log)
        for select in add_columns_selects:
            summary_select_clause.append(db.query(select).label(select.name))
        if not isinstance(entity_total_count_select, type(None)):
            summary_select_clause[1] = entity_total_count_select
        

    # Wrap everything in a subquery
    subquery = db.query(*summary_select_clause).subquery('json_result')
    query = db.query(func.row_to_json(subquery.table_valued()).label('results'))

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    start_time = time.time()
    result = query.all()
    result = [row for (row,) in result]
    query_time = time.time() - start_time
    log.info(f"Query execution time: {query_time}s")

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

    cols = []

    tablenames = DB_MAP.entity_tables.keys()

    # Step through columns in each table and use their ColumnInfo class to return required information
    for tablename in tablenames:
        columns = DB_MAP.get_table_column_infos(tablename)
        virtual_columns = DB_MAP.get_virtual_table_column_infos(tablename)
        columns.extend(virtual_columns)
        for column_info in columns:
            if column_info.data_returns:
                column = column_info.metadata_column
                # if column.name != "id_alias":
                col = dict()
                if column_info.virtual_table != None:
                    col["table"] = column_info.virtual_table
                else:
                    col["table"] = column_info.tablename

                col["column"] = column_info.uniquename
                col["data_type"] = str(column.type).lower()
                col["nullable"] = column.nullable
                col["description"] = column.comment
                cols.append(col)

    ret = {"result": cols}

    return ret


def column_values_query(db, column, data_source, limit, offset, log):
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

    column_info = DB_MAP.get_column_info(column)
    column = column_info.metadata_column

    column_values_query = db.query(column.label(column_info.uniquename), func.count().label("value_count")).group_by(column).order_by(column)

    if data_source:
        for source in data_source.split(','):
            source = source.strip()
            try:
                data_system_column = DB_MAP.get_meta_column(f"{column.table.name}_data_at_{source.lower()}")
                column_values_query = column_values_query.filter(data_system_column.is_(True))
            except Exception:
                error = SystemNotFound(f"system: {source} - not found")
                log.exception(error)
                raise error

    column_values_query = column_values_query.subquery("column_json")

    query = db.query(func.row_to_json(column_values_query.table_valued()))
    total_count_query = db.query(func.count()).select_from(column_values_query)

    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query, indented = True)}\n{"-"*60}')
    log.debug(f'Total Count Query:\n{"-"*100}\n{query_to_string(total_count_query, indented = True)}\n{"-"*100}')

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
    query = db.query(Base.metadata.tables["release_metadata"])
    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    # Return the results
    ret = {
        "result": [{"release_metadata": "success"}],
        "query_sql": query_to_string(query),
        "total_row_count": 0,
        "next_url": "",
    }
    return ret
