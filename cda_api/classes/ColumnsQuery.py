class ColumnsQuery():
    def __init__(self, db_info):
        self.db_info = db_info
        self.columns = []
        self.all_column_infos = []
        # Step through columns in each table and use their ColumnInfo class to return required information
        for table_info in self.db_info.data_table_infos:
            for column_info in table_info.get_data_column_infos():
                if column_info in self.all_column_infos:
                    continue
                self.all_column_infos.append(column_info)
                col = {}
                col["table"] = column_info.selectable_table_info.name
                col["column"] = column_info.name
                col["data_type"] = str(column_info.db_column.type).lower()
                col["nullable"] = column_info.db_column.nullable
                col["description"] = column_info.db_column.comment
                self.columns.append(col)

    def get_result(self):
        return {"result": self.columns}