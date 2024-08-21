from cda_api.db import get_db_map
from cda_api import get_logger
from .query_utilities import build_match_query, total_column_count_subquery, numeric_summary, categorical_summary, get_cte_column, data_source_counts, entity_count
from .filter_builder import build_match_conditons
from sqlalchemy import Label
log = get_logger()
DB_MAP = get_db_map()

def build_select_clause(entity_tablename, qnode):
    log.info('Building SELECT clause')
    add_columns = qnode.ADD_COLUMNS
    exclude_columns = qnode.EXCLUDE_COLUMNS
    select_columns = DB_MAP.get_metadata_table_columns(entity_tablename)
    collection_columns = []

    # Add additional columns to select list
    if add_columns:
        for additional_columnname in add_columns:
            additional_column = DB_MAP.get_meta_column(additional_columnname)
            if additional_column not in select_columns:
                log.debug(f'Adding {additional_columnname} to SELECT clause')
                select_columns.append(additional_column.label(additional_columnname))
                

    # Remove columns from select list
    if exclude_columns:
        for exclude_columnname in exclude_columns:
            exclude_column = DB_MAP.get_meta_column(exclude_columnname)
            if exclude_column in select_columns:
                log.debug(f'Removing {exclude_columnname} from SELECT clause')
                select_columns.remove(exclude_column)
                
    # Build out mapping column list for joins
    for column in select_columns:
        if isinstance(column, Label):
            column = column.element
        column_tablename = column.table.name
        if column_tablename != entity_tablename:
            log.debug(f'Mapping JOIN clause for {column.name}')
            relationship = DB_MAP.get_relationship(entity_tablename=entity_tablename, 
                                                          foreign_tablename=column_tablename)
            collection_column = relationship.entity_collection
            collection_columns.append(collection_column)

    return select_columns, collection_columns


def build_summary_select_clause(db, endpoint_tablename, qnode):
    match_all_conditions, match_some_conditions = build_match_conditons(endpoint_tablename, qnode)
    endpoint_columns = DB_MAP.get_metadata_table_columns(endpoint_tablename)
    endpoint_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    preselect_query = build_match_query(db=db,
                                        select_columns=endpoint_columns, 
                                        match_all_conditions=match_all_conditions,
                                        match_some_conditions=match_some_conditions)
    preselect_query = preselect_query.cte('filter_preselect')

    # Get total counts
    summary_select_clause = [total_column_count_subquery(db, get_cte_column(preselect_query, 'id_alias')).label('total_count')]
    
    # Get file/subject counts
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
    for column_info in endpoint_column_infos:
        column_summary = None
        preselect_column = get_cte_column(preselect_query, column_info.columnname)
        match column_info.category:
            case 'numeric':
                column_summary = numeric_summary(db, preselect_column)
                summary_select_clause.append(column_summary.label(f'{column_info.columnname}_summary'))
            case 'categorical':
                column_summary = categorical_summary(db, preselect_column)
                summary_select_clause.append(column_summary.label(f'{column_info.columnname}_summary'))
            case _:
                pass

    # Get data_source counts
    data_at_columns = [column for column in preselect_query.c if 'data_at' in column.name]
    data_count_select = data_source_counts(db, data_at_columns)
    summary_select_clause.append(data_count_select.label('data_source'))
    
    
    return summary_select_clause