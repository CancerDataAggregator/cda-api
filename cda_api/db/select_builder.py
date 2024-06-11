from .schema import Base, COLUMN_MAP
from .query_utilities import get_mapping_column
from cda_api import get_logger
log = get_logger()

def build_select_clause(entity_tablename, qnode):
    log.info('Building SELECT clause')
    add_columns = qnode.ADD_COLUMNS
    exclude_columns = qnode.EXCLUDE_COLUMNS
    select_columns = Base.metadata.tables[entity_tablename].columns.values()
    mapping_columns = []

    # Add additional columns to select list
    if add_columns:
        for additional_columnname in add_columns:
            additional_column = COLUMN_MAP[additional_columnname]['META_COLUMN']
            if additional_column not in select_columns:
                log.debug(f'Adding {additional_columnname} to SELECT clause')
                select_columns.append(additional_column)
                

    # Remove columns from select list
    if exclude_columns:
        for exclude_columnname in exclude_columns:
            exclude_column = COLUMN_MAP[exclude_columnname]['META_COLUMN']
            if exclude_column in select_columns:
                log.debug(f'Removing {exclude_columnname} from SELECT clause')
                select_columns.remove(exclude_column)
                

    # Build out mapping column list for joins
    for column in select_columns:
        column_tablename = column.table.name
        if column_tablename != entity_tablename:
            log.debug(f'Mapping JOIN clause for {column.name}')
            mapping_column = get_mapping_column(column_tablename, entity_tablename)
            mapping_columns.append(mapping_column)
    return select_columns, mapping_columns