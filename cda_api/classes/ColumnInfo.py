class ColumnInfo:
    def __init__(self, parent_table_info, name, db_column, column_metadata):
        self.parent_table_info = parent_table_info
        self.selectable_table_info = parent_table_info
        self.name = name
        self.db_column = db_column
        self.labeled_db_column = self.db_column.label(self.name)
        self.original_labeled_column = self.db_column.label(self.db_column.name)
        self.table_column_name = f"{self.db_column.table.name}.{self.db_column.name}"
        self.column_type = None
        self.summary_returns = False
        self.data_returns = False
        self.process_before_display = False
        self.virtual_table = None
        self.foreign_key_column_info = None
        
        if column_metadata is not None:
            self.column_type = column_metadata["column_type"]
            self.summary_returns = column_metadata["summary_returns"]
            self.data_returns = column_metadata["data_returns"]
            self.process_before_display = column_metadata["process_before_display"]
            self.virtual_table = column_metadata["virtual_table"]
        if self.virtual_table is not None:
            self.selectable_table_info = self.parent_table_info.database_info.get_table_info(self.virtual_table)
        self.null_column_info = None


    def __repr__(self):
        repr_components = [
            f'ColumnInfo({self.name})'
        ]
        return '\n'.join(repr_components)
    
    def assign_null_column(self, null_column_info):
        self.null_column_info = null_column_info


    def assign_foreign_key_column_infos(self):
        if len(self.db_column.foreign_keys) > 0:
            foreign_column = list(self.db_column.foreign_keys)[0].column
            self.foreign_key_column_info = self.parent_table_info.database_info.get_column_info(foreign_column)
