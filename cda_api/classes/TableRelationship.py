from sqlalchemy.sql import select, exists

class TableRelationship():
    def __init__(self, DB_MAP, primary_column, secondary_column, primary_mapping_column, secondary_mapping_column):
        self.db_map = DB_MAP
        self.primary_column_info = self.db_map.get_table_column_info(primary_column.table.name, primary_column.name)
        self.secondary_column_info = self.db_map.get_table_column_info(secondary_column.table.name, secondary_column.name)
        
        self._set_mapping_columns(primary_mapping_column, secondary_mapping_column)
        self._set_additional_filters()

    def __repr__(self):
        if self.primary_mapping_column_info is not None:
            return f"{self.primary_column_info.table_columnname} -> |{self.primary_mapping_column_info.table_columnname}|{self.secondary_mapping_column_info.table_columnname}| -> {self.secondary_column_info.table_columnname}"

        else:
            return f"{self.primary_column_info.table_columnname} -> {self.secondary_column_info.table_columnname}"
        
    def _set_mapping_columns(self, primary_mapping_column, secondary_mapping_column):
        if (primary_mapping_column is not None) and (secondary_mapping_column is not None):
            self.requires_mapping_table = True
            self.primary_mapping_column_info = self.db_map.get_table_column_info(primary_mapping_column.table.name, primary_mapping_column.name)
            self.secondary_mapping_column_info = self.db_map.get_table_column_info(secondary_mapping_column.table.name, secondary_mapping_column.name)
        else:
            self.requires_mapping_table = False
            self.primary_mapping_column_info = None
            self.secondary_mapping_column_info = None
            
    def _set_additional_filters(self):
        if self.secondary_column_info.tablename == 'upstream_identifiers':
            self.additional_filters = [self.secondary_column_info.metadata_table.c['cda_table'] == self.primary_column_info.tablename]
        else:
            self.additional_filters = []
    
    def get_preselect_filter_clause(self, secondary_filter_clause):
        if self.requires_mapping_table:
            subquery = select(1).select_from(self.secondary_mapping_column_info.metadata_table)\
                                .filter(self.primary_column_info.metadata_column == self.primary_mapping_column_info.metadata_column)\
                                .filter(self.secondary_mapping_column_info.metadata_column == self.secondary_column_info.metadata_column)
        else:
            subquery = select(1).select_from(self.secondary_column_info.metadata_table)\
                                .filter(self.primary_column_info.metadata_column == self.secondary_column_info.metadata_column)
        for additional_filter in self.additional_filters:
            subquery = subquery.filter(additional_filter)
        if secondary_filter_clause is not None:
            subquery = subquery.filter(secondary_filter_clause)
        return exists(subquery)