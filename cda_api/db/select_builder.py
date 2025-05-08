from sqlalchemy import Label, func

from cda_api.db import DB_MAP

from cda_api.classes.exceptions import TableNotFound

from .query_utilities import build_foreign_array_preselect, build_foreign_json_preselect, get_identifiers_preselect_columns, get_hanging_table_join


def build_data_select_clause(db, endpoint_tablename, request_body, filter_preselect_query, filter_table_map, log):
    log.info("Building select clause")
    add_columns = []
    if request_body.ADD_COLUMNS:
        add_columns.extend(request_body.ADD_COLUMNS)
    exclude_columns = request_body.EXCLUDE_COLUMNS
    table_column_infos = DB_MAP.get_table_column_infos(endpoint_tablename)
    virtual_table_column_infos = DB_MAP.get_virtual_table_column_infos(endpoint_tablename)
    table_column_infos.extend([column_info for column_info in virtual_table_column_infos if column_info.uniquename not in add_columns])
    log.debug(f"Adding '{endpoint_tablename}' default data columns to select clause. (Including virtual_table columns)")
    select_columns = [
        column_info.metadata_column.label(column_info.uniquename)
        for column_info in table_column_infos
        if column_info.data_returns
    ]
    foreign_array_map = {}
    foreign_joins = []
    identifiers = False
    added_columns = [column_info.uniquename for column_info in table_column_infos if column_info.data_returns]
    
    # Add additional columns to select list
    if add_columns:
        log.debug(f"Stepping through ADD_COLUMNS list")
        for add_columnname in add_columns:
            if add_columnname == f'{endpoint_tablename}_identifiers':
                identifiers = True
            else:
                add_column = DB_MAP.get_meta_column(add_columnname)
                if add_column not in added_columns:
                    log.debug(f"Adding {add_columnname} to SELECT clause")
                    select_columns.append(add_column.label(add_columnname))
                    added_columns.append(add_columnname)
                else:
                    log.debug(f"Skipping {add_columnname} since it is already in the select clause list")

    # Remove columns from select list
    to_remove = []
    if exclude_columns:
        log.debug(f"Stepping through EXCLUDE_COLUMNS list")
        for exclude_columnname in exclude_columns:
            for select_column in select_columns:
                if select_column.name == exclude_columnname:
                    log.debug(f"Removing {exclude_columnname} from SELECT clause")
                    to_remove.append(select_column)
    select_columns = [col for col in select_columns if col not in to_remove]

    # Build foreign_array_map to build a single preselect the columns in each foreign table
    for column in select_columns:
        unique_name = column.name
        if isinstance(column, Label):
            column = column.element
        if column.table.name != endpoint_tablename:
            if column.table.name not in foreign_array_map.keys():
                foreign_array_map[column.table.name] = [column.label(unique_name)]
            else:
                foreign_array_map[column.table.name].append(column.label(unique_name))

    # Build foreign array column preselects
    for foreign_tablename, columns in foreign_array_map.items():
        if request_body.EXPAND_RESULTS:
            # Need to add data_at columns here
            data_at_column_infos = [column_info for column_info in DB_MAP.get_table_column_infos(foreign_tablename) if column_info.process_before_display in ['data_source', 'data_source_count']]
            for data_at_col_info in data_at_column_infos:
                if data_at_col_info.uniquename not in [col.name for col in columns]:
                    columns.append(data_at_col_info.labeled_column)
            log.debug(f"Building expanded result for {foreign_tablename} columns: {[col.name for col in columns]}")
            foreign_join, preselect_columns = build_foreign_json_preselect(
                db, endpoint_tablename, foreign_tablename, columns, filter_preselect_query, filter_table_map, log
            )
            foreign_joins.append(foreign_join)
        else:
            log.debug(f"Building distinct array for {foreign_tablename} columns: {[col.name for col in columns]}")
            foreign_join, preselect_columns = build_foreign_array_preselect(
                db, endpoint_tablename, foreign_tablename, columns, filter_preselect_query, filter_table_map, log
            )
            foreign_joins.append(foreign_join)

        # Need to remove previous columns that were added to select_columns and replace them with the new preselect_columns
        to_remove = []
        for column in columns:
            for select_column in select_columns:
                if select_column.name == column.name:
                    to_remove.append(select_column)

        select_columns = [col for col in select_columns if col not in to_remove]

        for col in preselect_columns:
            hanging_table_join = get_hanging_table_join(endpoint_tablename, col)
            if hanging_table_join != None:
                foreign_joins.append(hanging_table_join)
            if request_body.EXPAND_RESULTS:
                select_columns.append(col.label(col.name))
            else:
                select_columns.append(func.coalesce(col, []).label(col.name))

    # TODO handle table_identifiers being added to exclude_columns
    if identifiers:
        log.debug(f"Adding provenance since '{endpoint_tablename}_identifiers' found in ADD_COLUMNS")
        foreign_join, preselect_columns = get_identifiers_preselect_columns(db, endpoint_tablename, filter_preselect_query, log)
        foreign_joins.append(foreign_join)
        for col in preselect_columns:
            select_columns.append(col.label(col.name))

    log.debug(f"Finished building select clause")
    return select_columns, foreign_joins
