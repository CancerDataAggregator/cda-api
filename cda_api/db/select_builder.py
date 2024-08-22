from cda_api.db import get_db_map
from cda_api import get_logger
from .query_utilities import build_match_query, build_foreign_array_preselect, total_column_count_subquery, numeric_summary, categorical_summary, get_cte_column, data_source_counts, entity_count
from .filter_builder import build_match_conditons
from sqlalchemy import Label
log = get_logger()
DB_MAP = get_db_map()

def build_fetch_rows_select_clause(db, entity_tablename, qnode, preselect_query):
    log.info('Building SELECT clause')
    add_columns = qnode.ADD_COLUMNS
    exclude_columns = qnode.EXCLUDE_COLUMNS
    table_column_infos = DB_MAP.get_table_column_infos(entity_tablename)
    select_columns = [column_info.metadata_column for column_info in table_column_infos if column_info.fetch_rows_returns]
    foreign_array_map = {}
    foreign_array_preselects = []
    foreign_joins = []

    # Add additional columns to select list
    if add_columns:
        for add_columnname in add_columns:
            add_column = DB_MAP.get_meta_column(add_columnname)
            if add_column not in select_columns:
                log.debug(f'Adding {add_columnname} to SELECT clause')
                select_columns.append(add_column.label(add_columnname))

    # Remove columns from select list
    if exclude_columns:
        for exclude_columnname in exclude_columns:
            exclude_column = DB_MAP.get_meta_column(exclude_columnname)
            if exclude_column in select_columns:
                to_remove = []
                for select_column in select_columns:
                    if select_column.name == exclude_column.name:
                        log.debug(f'Removing {exclude_columnname} from SELECT clause')
                        to_remove.append(select_column)
                select_columns = [col for col in select_columns if col not in to_remove]

    # Build foreign_array_map
    for column in select_columns:
        if isinstance(column, Label):
            unique_name = column.name
            column = column.element
        if column.table.name != entity_tablename:
            if column.table.name not in foreign_array_map.keys():
                foreign_array_map[column.table.name] = [column.label(unique_name)]
            else:
                foreign_array_map[column.table.name].append(column.label(unique_name))

    # Build foreign array columns
    for foreign_tablename, columns in foreign_array_map.items():
        foreign_array_preselect, foreign_join, preselect_columns = build_foreign_array_preselect(db, entity_tablename, foreign_tablename, columns, preselect_query)
        foreign_array_preselects.append(foreign_array_preselect)
        foreign_joins.append(foreign_join)
        # Need to remove previous columns that were added to select_columns and replace them with the new preselect_columns
        for column in columns:
            to_remove = []
            for select_column in select_columns:
                if select_column.name == column.name:
                    to_remove.append(select_column)
            select_columns = [col for col in select_columns if col not in to_remove]
        select_columns += preselect_columns

    return select_columns, foreign_array_preselects, foreign_joins


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
        if column_info.summary_display and  column_info.process_before_display != 'data_source':
            match column_info.column_type:
                case 'numeric':
                    column_summary = numeric_summary(db, preselect_column)
                    summary_select_clause.append(column_summary.label(f'{column_info.columnname}_summary'))
                case 'categorical':
                    column_summary = categorical_summary(db, preselect_column)
                    summary_select_clause.append(column_summary.label(f'{column_info.columnname}_summary'))
                case _:
                    log.warning(f'Unexpectedly skipping {column_info.column_name} for summary - column_type: {column_info.column_type}')
                    pass

    # Get data_source counts
    table_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    data_source_columnnames = [column_info.columnname for column_info in table_column_infos if column_info.process_before_display == 'data_source']
    data_source_columns = [column for column in preselect_query.c if column.name in data_source_columnnames]
    data_source_count_select = data_source_counts(db, data_source_columns)
    summary_select_clause.append(data_source_count_select.label('data_source'))
    
    return summary_select_clause