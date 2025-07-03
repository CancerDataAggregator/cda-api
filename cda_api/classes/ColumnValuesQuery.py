from cda_api import SystemNotFound
from cda_api.classes.DatabaseInfo import DatabaseInfo
from sqlalchemy import func


class ColumnValuesQuery:
    def __init__(self, db, db_info: DatabaseInfo, column_name, data_source_string, log):
        self.db = db
        self.db_info = db_info

        column_info = self.db_info.get_column_info(column_name)

        column_values_query = db.query(column_info.labeled_db_column, func.count().label("value_count")).group_by(column_info.db_column).order_by(column_info.db_column)

        if data_source_string:
            for source in data_source_string.split(','):
                source = source.strip()
                try:
                    data_system_column_info = self.db_info.get_column_info(f"{column_info.selectable_table_info.name}_data_at_{source.lower()}")
                    column_values_query = column_values_query.filter(data_system_column_info.db_column.is_(True))
                except Exception:
                    error = SystemNotFound(f"system: {source} - not found")
                    log.exception(error)
                    raise error

        self.column_values_query = column_values_query.subquery("column_json")

    def get_query(self):
        query = self.db.query(func.row_to_json(self.column_values_query.table_valued()))
        return query

    def get_total_count_query(self):
        total_count_query = self.db.query(func.count()).select_from(self.column_values_query)
        return total_count_query