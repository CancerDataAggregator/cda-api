from sqlalchemy import Label
from cda_api import ColumnNotFound
from cda_api.db.query_utilities import add_hanging_table_joins, build_filter_preselect
from cda_api.db.select_builder import build_fetch_rows_select_clause
from cda_api.db.filter_builder import build_match_conditons, parse_filter_string
from cda_api.db import DB_MAP
from cda_api.models import QNode

def get_dicom_series_qnode(qnode, log):
    if qnode.MATCH_ALL:
        match_all = [filter for filter in qnode.MATCH_ALL]
    else: 
        match_all = []
    if qnode.MATCH_SOME:
        match_some = [filter for filter in qnode.MATCH_SOME]
    else:
        match_some = []
    if qnode.ADD_COLUMNS:
        add_columns = [columnname for columnname in qnode.ADD_COLUMNS]
    else:
        add_columns = []
    if qnode.EXCLUDE_COLUMNS:
        exclude_columns = [columnname for columnname in qnode.EXCLUDE_COLUMNS]
    else:
        exclude_columns = []
    ds_match_all = []
    ds_match_some = []
    ds_add_columns = []
    ds_exclude_columns = []
    for filter in match_all:
        columnname, _, _ = parse_filter_string(filter, log)
        if columnname in DB_MAP.file_dicom_column_map.keys():
            dicom_columnname = DB_MAP.file_dicom_column_map[columnname].uniquename
            ds_match_all.append(filter.replace(columnname, dicom_columnname))
        else:
            ds_match_all.append(filter)
    for filter in match_some:
        columnname, _, _ = parse_filter_string(filter, log)
        if columnname in DB_MAP.file_dicom_column_map.keys():
            dicom_columnname = DB_MAP.file_dicom_column_map[columnname].uniquename
            ds_match_some.append(filter.replace(columnname, dicom_columnname))
        else:
            ds_match_some.append(filter)
    for columnname in add_columns:
        if columnname in DB_MAP.file_dicom_column_map.keys():
            dicom_columnname = DB_MAP.file_dicom_column_map[columnname].uniquename
            ds_add_columns.append(dicom_columnname)
        else:
            ds_add_columns.append(columnname)
    for columnname in exclude_columns:
        if columnname in DB_MAP.file_dicom_column_map.keys():
            dicom_columnname = DB_MAP.file_dicom_column_map[columnname].uniquename
            ds_exclude_columns.append(dicom_columnname)
        else:
            ds_exclude_columns.append(columnname)
    return QNode(MATCH_ALL=ds_match_all, MATCH_SOME=ds_match_some, ADD_COLUMNS=ds_add_columns, EXCLUDE_COLUMNS=ds_exclude_columns)

def get_dicom_series_fetch_rows_query(db, select_columns, qnode, endpoint_tablename, log):
    dicom_qnode = get_dicom_series_qnode(qnode, log)
    if dicom_qnode == qnode:
        log.debug('No file column being selected so no need to relate to dicom_series')
        return None
    
    if endpoint_tablename == 'file':
        tablename = 'dicom_series'
    else:
        tablename = endpoint_tablename
    
    ds_match_all_conditions, ds_match_some_conditions = build_match_conditons(tablename, dicom_qnode, log)
    dicom_preselect_query, dicom_id_alias = build_filter_preselect(db, tablename, ds_match_all_conditions, ds_match_some_conditions, dicom_flag=True)
    preselect_columns, ds_foreign_array_preselects, ds_foreign_joins = build_fetch_rows_select_clause(db, tablename, dicom_qnode, dicom_preselect_query, log, dicom_flag=True)

    if endpoint_tablename != 'file':
        dicom_select_columns = []
        for column in preselect_columns:
            for file_col_uniquename, dicom_column_info in DB_MAP.file_dicom_column_map.items():
                if dicom_column_info.uniquename == column.name:
                    column = column.label(file_col_uniquename)
                    break
            dicom_select_columns.append(column)
    else:
        dicom_select_columns = []
        for column in select_columns:
            uniquename = column.name
            
            if isinstance(column, Label):
                column = column.element
            if uniquename in DB_MAP.file_dicom_column_map.keys():
                dicom_uniquename = DB_MAP.file_dicom_column_map[uniquename].uniquename
                if DB_MAP.get_column_info(uniquename).tablename == 'file':
                    dicom_select_columns.append(DB_MAP.file_dicom_column_map[uniquename].metadata_column.label(uniquename))
                elif dicom_uniquename in [column.name for column in preselect_columns]:
                    column_preselect = [column for column in preselect_columns if column.name == dicom_uniquename][0]
                    dicom_select_columns.append(column_preselect.label(uniquename))
                else:
                    raise ColumnNotFound(f'Could not find column: {dicom_uniquename} in either dicom_series, or in the foreign array preselects')
            else:
                if uniquename in [column.name for column in preselect_columns]:
                    column_preselect = [column for column in preselect_columns if column.name == uniquename][0]
                    dicom_select_columns.append(column_preselect.label(uniquename))
                else:
                    dicom_select_columns.append(column)
    
    
    query = db.query(*dicom_select_columns)
    query = query.filter(dicom_id_alias.in_(dicom_preselect_query))

    # Add joins to foreign table preselects
    if ds_foreign_joins:
        for foreign_join in ds_foreign_joins:
            query = query.join(**foreign_join, isouter=True)


    query = add_hanging_table_joins('dicom_series', dicom_select_columns, query)
    count_subquery = db.query(dicom_id_alias).filter(dicom_id_alias.in_(dicom_preselect_query)).subquery('rows_to_count_dicom')

    return query, count_subquery