class TableRelationship():
    def __init__(self, local_column_info, foreign_column_info, local_mapping_column_info, foreign_mapping_column_info):
        self.local_column_info = local_column_info
        self.foreign_column_info = foreign_column_info
        self.local_mapping_column_info = local_mapping_column_info
        self.foreign_mapping_column_info = foreign_mapping_column_info
        self.requires_mapping_table = False
        if (self.local_mapping_column_info is not None) and (self.foreign_mapping_column_info is not None):
            self.requires_mapping_table = True
        self._set_additional_filters()

    def __repr__(self):
        if self.local_mapping_column_info is not None:
            return f"{self.local_column_info.table_column_name} -> |{self.local_mapping_column_info.table_column_name}|{self.foreign_mapping_column_info.table_column_name}| -> {self.foreign_column_info.table_column_name}"

        else:
            return f"{self.local_column_info.table_column_name} -> {self.foreign_column_info.table_column_name}"
            
    def _set_additional_filters(self):
        if self.foreign_column_info.parent_table_info.name == 'upstream_identifiers':
            cda_table_column_info = self.foreign_column_info.parent_table_info.get_column_info('cda_table')
            self.additional_filters = [cda_table_column_info.db_column == self.local_column_info.parent_table_info.name]
        else:
            self.additional_filters = []