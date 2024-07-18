from .schema import get_db_map
from cda_api import get_logger
from .query_utilities import build_match_query, total_count, grouped_count, get_cte_column, data_source_counts, entity_file_count
from .filter_builder import build_match_conditons
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
                select_columns.append(additional_column)
                

    # Remove columns from select list
    if exclude_columns:
        for exclude_columnname in exclude_columns:
            exclude_column = DB_MAP.get_meta_column(exclude_columnname)
            if exclude_column in select_columns:
                log.debug(f'Removing {exclude_columnname} from SELECT clause')
                select_columns.remove(exclude_column)
                
    # Build out mapping column list for joins
    for column in select_columns:
        column_tablename = column.table.name
        if column_tablename != entity_tablename:
            log.debug(f'Mapping JOIN clause for {column.name}')
            relationship = DB_MAP.get_relationship(entity_tablename=entity_tablename, 
                                                          foreign_tablename=column_tablename)
            collection_column = relationship.entity_collection
            collection_columns.append(collection_column)

    return select_columns, collection_columns


def build_summary_select_clause(db, endpoint_tablename, qnode):
    column_map = DB_MAP.column_map
    # Temporary summary column mapping
    SUMMARY_MAP = {
        'subject': {
            'total_count_columns': [column_map['subject_integer_id_alias']],
            'grouped_count_columns': [column_map['race'], 
                                    column_map['ethnicity'],
                                    column_map['cause_of_death']]
        }
    }

    # Build preselect filter with select columns from temporary summary map
    match_all_conditions, match_some_conditions = build_match_conditons(endpoint_tablename, qnode)
    summary_select_clause = []
    total_count_columns = SUMMARY_MAP[endpoint_tablename]['total_count_columns']
    grouped_count_columns = SUMMARY_MAP[endpoint_tablename]['grouped_count_columns']
    preselect_select_columns = [column_info.metadata_column for column_info in 
                                total_count_columns + grouped_count_columns
                                if column_info.tablename == endpoint_tablename]
    preselect_select_columns += [column for column in DB_MAP.get_metadata_table_columns(endpoint_tablename)
                                 if 'data_at' in column.name]
    preselect_query = build_match_query(db=db,
                                        select_columns=preselect_select_columns, 
                                        match_all_conditions=match_all_conditions,
                                        match_some_conditions=match_some_conditions)
    preselect_query = preselect_query.cte('filter_preselect')

    # Get total counts
    for column_info in total_count_columns:
        if 'id_alias' in column_info.columnname:
            alias = 'total_count'
        else:
            alias = f"total_{column_info.columnname}"
        column_summary = total_count(db, get_cte_column(preselect_query, column_info.columnname)).label(alias)
        if summary_select_clause:
            summary_select_clause.append(column_summary)
        else:
            summary_select_clause = [column_summary]

    # Get file counts
    file_count = entity_file_count(db=db,
                                   endpoint_tablename=endpoint_tablename, 
                                    preselect_query=preselect_query)
    summary_select_clause.append(file_count.label('file_count'))

    # Get categorical(grouped) column counts
    for column_info in grouped_count_columns:
        alias = f"{column_info.columnname}_count"
        column_summary = grouped_count(db, get_cte_column(preselect_query, column_info.columnname)).label(alias)
        summary_select_clause.append(column_summary)

    # Get data_source counts
    data_at_columns = [column for column in preselect_query.c if 'data_at' in column.name]
    data_count_select = data_source_counts(db, data_at_columns)
    summary_select_clause.append(data_count_select.label('data_source'))

    return summary_select_clause