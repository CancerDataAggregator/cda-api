from .ColumnInfo import ColumnInfo
from .TableInfo import TableInfo
from .TableRelationship import TableRelationship
from cda_api import get_logger, TableNotFound, ColumnNotFound, RelationshipNotFound
from cda_api.db.connection import session
from sqlalchemy import func
from sqlalchemy.sql.schema import Column, Table

log = get_logger('DatabaseInfo.py')
setup_log = get_logger("Setup: DatabaseMap.py")

class DatabaseInfo:
    def __init__(self, db_base):
        self.db_base = db_base
        self._build_sqlalchemy_components()
        self._build_column_metadata_map()
        self._build_table_infos()
        self._build_table_relationships()
        self._assign_virtual_table_columns()
        self._assign_null_columns()
        self._assign_foreign_key_column_infos()
        self._assign_primary_table_infos()
        
    def _build_sqlalchemy_components(self):
        setup_log.info("Building variables from automapped Base")
        self.db_tables = self.db_base.metadata.tables
        self.table_names = [table.name for table in self.db_tables.values()]
        self.db_columns = [column for table in self.db_tables.values() for column in table.columns]
        self.column_names = [column.name for column in self.db_columns]

    def _build_column_metadata_map(self):
        # Fetch column_metadata and build a map of table.column to their respective metadata
        setup_log.info("Fetching info from the column_metadata table")
        column_metadata = self.db_tables["column_metadata"]
        db = session()
        subquery = db.query(column_metadata).subquery("json_result")
        query = db.query(func.row_to_json(subquery.table_valued()))
        result = query.all()
        result = [row for (row,) in result]
        self.column_metadata_map = {}
        for row in result:
            table_name = row["cda_table"]
            column_name = row["cda_column"]
            metadata = {k: v for k, v in row.items() if k not in ["cda_table", "cda_column"]}
            if table_name not in self.column_metadata_map.keys():
                self.column_metadata_map[table_name] = {}
            if column_name not in self.column_metadata_map[table_name].keys():
                self.column_metadata_map[table_name][column_name] = metadata
    
    def _build_table_infos(self):
        self.table_infos = []
        self.local_table_infos = []
        self.data_table_infos = []
        self.standalone_table_infos = []
        self.mapping_table_infos = []
        self.all_column_infos = []
        all_duplicate_column_names = list(set([column_name for column_name in self.column_names if self.column_names.count(column_name) > 1]))
        for db_table in self.db_tables.values():
            table_duplicate_column_names = [column.name for column in db_table.columns if column.name in all_duplicate_column_names]
            table_column_metadata = {}
            if db_table.name in self.column_metadata_map.keys():
                table_column_metadata = self.column_metadata_map[db_table.name]
            table_info = TableInfo(self, db_table, table_column_metadata, table_duplicate_column_names, log)
            self.table_infos.append(table_info)
            if table_info.name in ['file', 'subject']:
                self.local_table_infos.append(table_info)
            if table_info.name not in ['release_metadata', 'column_metadata']:
                if len(table_info.db_table.foreign_keys) < 2:
                    self.data_table_infos.append(table_info)
                else:
                    self.mapping_table_infos.append(table_info)
            self.all_column_infos.extend(table_info.column_infos)
                    
    
    def _build_table_relationships(self):
        for local_table_info in self.local_table_infos:
            for data_table_info in self.data_table_infos:
                if local_table_info == data_table_info:
                    continue
                local_table_info.build_table_relationship(data_table_info)
    
    def _assign_virtual_table_columns(self):
        for column_info in self.all_column_infos:
            if column_info.virtual_table is not None:
                table_info = self.get_table_info(column_info.virtual_table)
                table_info.add_virtual_table_columns(column_info)

    def _assign_null_columns(self):
        for column_info in self.all_column_infos:
            if column_info.name.endswith('null') and column_info.parent_table_info.name.endswith('nulls'):
                column_info_to_assign = self.get_column_info(column_info.name.replace('_null', ''), column_info.parent_table_info.name.replace('_nulls', ''))
                column_info_to_assign.assign_null_column(column_info)
    
    def _assign_foreign_key_column_infos(self):
        for column_info in self.all_column_infos:
            column_info.assign_foreign_key_column_infos()

    def _assign_primary_table_infos(self):
        for table_info in self.table_infos:
            table_info.set_primary_table_info()
    
    def get_column_info(self, column, table = None) -> ColumnInfo:
        if table is None:
            potential_column_infos = []
            if isinstance(column, str):
                potential_column_infos = [column_info for column_info in self.all_column_infos if column_info.name == column]
            elif isinstance(column, Column):
                potential_column_infos = [column_info for column_info in self.all_column_infos if column_info.db_column == column]

            if len(potential_column_infos) < 1:
                # TODO raise better exceptions
                raise ColumnNotFound(f"Column Not Found: {column}")
            elif len(potential_column_infos) > 1:
                # TODO raise better exceptions
                raise ColumnNotFound(f"Unexpectedly found more that one column named: {column}")
            return potential_column_infos[0]
        
        else:
            table_info = self.get_table_info(table)
            return table_info.get_column_info(column)
        
    def get_table_info(self, table) -> TableInfo:
        if isinstance(table, str):
            potential_table_infos = [table_info for table_info in self.table_infos if table_info.name == table]
        elif isinstance(table, Table):
            potential_table_infos = [table_info for table_info in self.table_infos if table_info.db_table == table]
        elif isinstance(table, TableInfo):
            return table
        else:
            raise Exception(f"Unexpected type {type(table)} for foreign_table. Only expecting str, Table, or TableInfo")
        if len(potential_table_infos) < 1:
            # TODO raise better exceptions
            raise TableNotFound(f"Table not found: {table}")
        elif len(potential_table_infos) > 1:
            # TODO raise better exceptions
            raise TableNotFound(f"Unexpectedly found more that one table named: {table}")
        return potential_table_infos[0]
    
    def get_table_relationship(self, local_table, foreign_table) -> TableRelationship:
        local_table_info = self.get_table_info(local_table)
        if local_table_info not in self.local_table_infos:
            raise RelationshipNotFound(f'Unexpected local table: {local_table}. Should only be from following list of tables {[table_info.name for table_info in self.local_table_infos]}')

        return local_table_info.get_table_relationship(foreign_table)
