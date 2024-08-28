from cda_api.db import get_db_map
from cda_api import get_logger
from .query_utilities import build_foreign_array_preselect
from sqlalchemy import Label

DB_MAP = get_db_map()

def build_fetch_rows_select_clause(db, entity_tablename, qnode, filter_preselect_query, log):
    log.info('Building SELECT clause')
    add_columns = qnode.ADD_COLUMNS
    exclude_columns = qnode.EXCLUDE_COLUMNS
    table_column_infos = DB_MAP.get_table_column_infos(entity_tablename)
    select_columns = [column_info.metadata_column.label(column_info.uniquename) for column_info in table_column_infos if column_info.fetch_rows_returns]
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
    to_remove = []
    if exclude_columns:
        for exclude_columnname in exclude_columns:
            for select_column in select_columns:
                if select_column.name == exclude_columnname:
                    log.debug(f'Removing {exclude_columnname} from SELECT clause')
                    to_remove.append(select_column)
    select_columns = [col for col in select_columns if col not in to_remove]

    # Build foreign_array_map to build a single preselect the columns in each foreign table
    for column in select_columns:
        if isinstance(column, Label):
            unique_name = column.name
            column = column.element
        if column.table.name != entity_tablename:
            if column.table.name not in foreign_array_map.keys():
                foreign_array_map[column.table.name] = [column.label(unique_name)]
            else:
                foreign_array_map[column.table.name].append(column.label(unique_name))

    # Build foreign array column preselects
    for foreign_tablename, columns in foreign_array_map.items():
        foreign_array_preselect, foreign_join, preselect_columns = build_foreign_array_preselect(db, entity_tablename, foreign_tablename, columns, filter_preselect_query)
        foreign_array_preselects.append(foreign_array_preselect)
        foreign_joins.append(foreign_join)

        # Need to remove previous columns that were added to select_columns and replace them with the new preselect_columns
        to_remove = []
        for column in columns:
            for select_column in select_columns:
                if select_column.name == column.name:
                    to_remove.append(select_column)
        
        select_columns = [col for col in select_columns if col not in to_remove]

        # Add preselect columns
        select_columns += preselect_columns

    return select_columns, foreign_array_preselects, foreign_joins
