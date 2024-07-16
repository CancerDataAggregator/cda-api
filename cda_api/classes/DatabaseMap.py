from cda_api import ColumnNotFound, RelationshipNotFound, TableNotFound, get_logger
from .ColumnInfo import ColumnInfo
from .EntityRelationship import EntityRelationship
from sqlalchemy import inspect, Column
log = get_logger()


class DatabaseMap():
    def __init__(self, db_base):
        self.db_base = db_base
        self._build_metadata_variables()
        self._build_entity_table_variables()
        self._build_column_map()
        self._build_relationship_map()

    def _build_metadata_variables(self):
        self.metadata_tables = self.db_base.metadata.tables
        self.metadata_columns = [column for table in self.metadata_tables.values()
                                        for column in table.columns]
        self.metadata_column_names = [column.name for column in self.metadata_columns]

    def _build_entity_table_variables(self):
        self.entity_tables = self.db_base.classes
        self.entity_tablenames = self.entity_tables.keys()
        self.entity_columnnames = [column.name for column in self.metadata_columns 
                                   if column.table.name in self.entity_tablenames]
        self.entity_table_column_map = {tablename: self.metadata_tables[tablename].columns.values()
                                        for tablename in self.entity_tablenames} 

    def _build_column_map(self):
        self.column_map = {}

        duplicate_column_names = list(set([columnname for columnname in self.metadata_column_names
                                            if self.metadata_column_names.count(columnname) > 1]))
        
        for metadata_tablename, metadata_table in self.metadata_tables.items():
            if metadata_tablename in self.entity_tablenames:
                entity_table = self.entity_tables[metadata_tablename]
            else:
                entity_table = None
            for metadata_column in metadata_table.columns:
                if metadata_column.name in duplicate_column_names:
                    uniquename = f'{metadata_tablename}_{metadata_column.name}'
                else:
                    uniquename = metadata_column.name
                
                self.column_map[uniquename] = ColumnInfo(unqiuename=uniquename,
                                                         entity_table=entity_table, 
                                                         metadata_table=metadata_table, 
                                                         metadata_column=metadata_column)
                
    def _build_relationship_map(self):
        self.relationship_map = {}
        for tablename, table in self.entity_tables.items():
            self.relationship_map[tablename] = {}
            i = inspect(table)
            for relationship in i.relationships:
                if tablename != relationship.target.name:
                    self.relationship_map[tablename][relationship.target.name] = EntityRelationship(tablename, relationship)
        

    def get_column_info(self, columnname) -> ColumnInfo:
        try: 
            return self.column_map[columnname]
        except Exception as e:
            possible_cols = [k for k in self.column_map.keys() if k.endswith(columnname)]
            possible_cols.extend([k for k in self.column_map.keys() if k.startswith(columnname)])
            if possible_cols:
                log_message = f'Column Not Found: {columnname}, did you mean: {possible_cols}\n{e}'
            else:
                log_message = f'Column Not Found: {columnname}\n{e}'
            log.exception(log_message)
            raise ColumnNotFound(log_message)
        
    def get_meta_column(self, columnname) -> Column:
        try: 
            return self.column_map[columnname].metadata_column
        except Exception as e:
            possible_cols = [k for k in self.column_map.keys() if k.endswith(columnname)]
            possible_cols.extend([k for k in self.column_map.keys() if k.startswith(columnname)])
            if possible_cols:
                log_message = f'Column Not Found: {columnname}, did you mean: {possible_cols}\n{e}'
            else:
                log_message = f'Column Not Found: {columnname}\n{e}'
            log.exception(log_message)
            raise ColumnNotFound(log_message)

    def get_relationship(self, entity_tablename, foreign_tablename) -> EntityRelationship:
        try:
            return self.relationship_map[entity_tablename][foreign_tablename]
        except Exception as e:
            log_message = f'Unable to find relationship between {entity_tablename} and {foreign_tablename}\n{e}'
            log.exception(log_message)
            raise RelationshipNotFound(log_message)
        
    def get_entity_table(self, entity_tablename):
        try:
            return self.entity_tables[entity_tablename]
        except Exception as e:
            log_message = f'Unable to find entity table {entity_tablename}\n{e}'
            log.exception(log_message)
            raise TableNotFound(log_message)
        
    def get_metadata_table(self, tablename):
        try:
            return self.metadata_tables[tablename]
        except Exception as e:
            log_message = f'Unable to find entity table {tablename}\n{e}'
            log.exception(log_message)
            raise TableNotFound(log_message)
        
    def get_metadata_table_columns(self, tablename):
        try:
            return self.metadata_tables[tablename].columns.values()
        except Exception as e:
            log_message = f'Unable to find entity table {tablename}\n{e}'
            log.exception(log_message)
            raise TableNotFound(log_message)

    def get_table_column_infos(self, tablename):
        try:
            return [column_info for column_info in self.column_map.values() if column_info.tablename == tablename]
        except ColumnNotFound as cnf:
            raise cnf
        except Exception as e:
            log_message = f'Unable to find entity table {tablename}\n{e}'
            log.exception(log_message)
            raise TableNotFound(log_message)
