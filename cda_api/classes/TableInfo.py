from .ColumnInfo import ColumnInfo
from .TableRelationship import TableRelationship
from sqlalchemy.sql.schema import Column, Table
from cda_api import RelationshipNotFound, RelationshipError, ColumnNotFound, get_logger

class TableInfo:
    def __init__(self, database_info, db_table, table_column_metadata, table_duplicate_column_names, log):
        self.database_info = database_info
        self.db_table = db_table
        self.name = self.db_table.name
        self.log = get_logger('TableInfo.py')
        self.db_columns = [column for column in db_table.columns]
        self.foreign_key_map = {foreign_key.column.table.name: foreign_key for foreign_key in db_table.foreign_keys}
        self.primary_key_column_info = None 
        self.relationship_map = {}
        self.virtual_column_infos = []
        self.primary_table_info = None
        
        self._build_column_info_list(table_column_metadata, table_duplicate_column_names)

    def __repr__(self):
        return f"TableInfo({self.name})"
    
    def _build_column_info_list(self, table_column_metadata, table_duplicate_column_names):
        unique_name_overwrite = {
            'file_anatomic_site_anatomic_site': 'anatomic_site',
        }
        self.column_infos = []
        for db_column in self.db_columns:
            unique_name = db_column.name
            column_metadata = None
            if db_column.name in table_duplicate_column_names:
                unique_name = f'{self.name}_{db_column.name}'
            if unique_name in unique_name_overwrite.keys():
                unique_name = unique_name_overwrite[unique_name]
            if db_column.name in table_column_metadata.keys():
                column_metadata = table_column_metadata[db_column.name]
            
            column_info = ColumnInfo(self, unique_name, db_column, column_metadata)
            if len(self.db_table.primary_key.columns) > 0:
                if db_column == self.db_table.primary_key.columns[0]:
                    self.primary_key_column_info = column_info
            self.column_infos.append(column_info)

    def set_primary_table_info(self):
        if self.foreign_key_map:
            if len(self.foreign_key_map.keys()) == 1:
                self.primary_table_info = self.database_info.get_table_info(list(self.foreign_key_map.keys())[0])
        else:
            self.primary_table_info = self
        
    
    def build_table_relationship(self, foreign_table_info):
        self.log.debug(f'Mapping relationship between {self} and {foreign_table_info}')
        local_column_info = None
        foreign_column_info = None
        local_mapping_column_info = None
        foreign_mapping_column_info = None
        # Direct connection
        if self.name in foreign_table_info.foreign_key_map.keys():
            foreign_key = foreign_table_info.foreign_key_map[self.name]
            local_column_info = self.get_column_info(foreign_key.column.name)
            foreign_column_info = self.database_info.get_column_info(foreign_key.parent)

        elif foreign_table_info in self.database_info.mapping_table_infos:
            if self.name in foreign_table_info.foreign_key_map.keys():
                foreign_key = foreign_table_info.foreign_key_map[foreign_table_info.name]
                local_column_info = self.get_column_info(foreign_key.column)
                foreign_column_info = self.database_info.get_column_info(foreign_key.parent)
        
        # Direct connection but doesn't show in foreign_keys
        elif foreign_table_info.name == 'upstream_identifiers': 
            local_column_info = self.get_column_info(self.db_table.primary_key.c[0].name)
            foreign_column_info = self.database_info.get_column_info('id_alias', foreign_table_info.name)
 
        # Mapping columns required
        if local_column_info is None or foreign_column_info is None:
            # Find all potential paths through the mapping tables
            potential_local_fks = []
            potential_foreign_table_fks = []
            potential_foreign_table_columns = []

            for mapping_table_info in self.database_info.mapping_table_infos:
                potential_local_fk = None
                potential_foreign_table_fk = None
                if self.name in mapping_table_info.foreign_key_map.keys():
                    potential_local_fk = mapping_table_info.foreign_key_map[self.name]
                    if foreign_table_info.name in mapping_table_info.foreign_key_map.keys():
                        potential_foreign_table_fk = mapping_table_info.foreign_key_map[foreign_table_info.name]
                    else:
                        # This covers cases list subject -> file_tumor_vs_normal where there is no mapping table directly linking them but we can use file_describes_subject
                        for mapping_fk in mapping_table_info.foreign_key_map.values():
                            for foreign_table_fk in foreign_table_info.foreign_key_map.values():
                                if foreign_table_fk.column == mapping_fk.column:
                                    potential_foreign_table_columns.append(foreign_table_fk.parent)
                                    potential_foreign_table_fk = mapping_fk
                # Only if we find a connection to both the local and foreign table do we append to the potential foreign key lists
                if potential_local_fk and potential_foreign_table_fk:
                    potential_local_fks.append(potential_local_fk)
                    potential_foreign_table_fks.append(potential_foreign_table_fk)
                
            potential_local_fks = list(set(potential_local_fks))
            potential_foreign_table_fks = list(set(potential_foreign_table_fks))
            potential_foreign_table_columns = list(set(potential_foreign_table_columns))
            # Check 
            if potential_local_fks and potential_foreign_table_fks:
                if len(potential_local_fks) > 1 or len(potential_foreign_table_fks) > 1:
                    raise RelationshipError(f'Unexpectedly found more than one path between {self.name}, {foreign_table_info.name}')
                
                local_fk = potential_local_fks[0]
                foreign_table_fk = potential_foreign_table_fks[0]
                
                if local_fk == foreign_table_fk:
                    raise RelationshipError(f'Unexpectedly found relationship path where {self.name}, {foreign_table_info.name} relate via the same foreign key')
                
                local_column_info = self.get_column_info(local_fk.column.name)
                local_mapping_column_info = self.database_info.get_column_info(local_fk.parent)

                if potential_foreign_table_columns:
                    if len(potential_foreign_table_columns) > 1:
                        raise RelationshipError(f'Unexpectedly found more than one potential secondary column {self.name}, {foreign_table_info.name}')
                    foreign_column = potential_foreign_table_columns[0]
                    foreign_column_info = self.database_info.get_column_info(foreign_column)
                else:
                    foreign_column_info = self.database_info.get_column_info(foreign_table_fk.column)
                foreign_mapping_column_info = self.database_info.get_column_info(foreign_table_fk.parent)
                        

        if local_column_info is None or foreign_column_info is None:
            if self.name == 'file' and foreign_table_info.name == 'external_reference':
                return
            raise RelationshipError(f'Unable to find a path between {self.name}, {foreign_table_info.name}')

        table_relationship = TableRelationship(local_column_info, foreign_column_info, local_mapping_column_info, foreign_mapping_column_info)
        self.relationship_map[foreign_table_info.name] = table_relationship
        self.log.debug(f'Built: {table_relationship}')

    def add_virtual_table_columns(self, virtual_table_column_info):
        self.log.debug(f'Adding {virtual_table_column_info.name} to {self.name}')
        self.virtual_column_infos.append(virtual_table_column_info)


    def get_column_info(self, column) -> ColumnInfo:
        if isinstance(column, str):
            potential_column_infos = [column_info for column_info in self.column_infos if column_info.name == column]
        elif isinstance(column, Column):
            potential_column_infos = [column_info for column_info in self.column_infos if column_info.db_column == column]
        if len(potential_column_infos) < 1:
            # TODO raise better exceptions
            potential_column_infos = [column_info for column_info in self.column_infos if column_info.db_column.name == column]
            if len(potential_column_infos) < 1:
                raise ColumnNotFound(f"Column Not Found: {column} in table {self.name}")
        elif len(potential_column_infos) > 1:
            # TODO raise better exceptions
            raise Exception(f"Unexpectedly found more that one column named: {column}")
        
        return potential_column_infos[0]

    def get_table_relationship(self, foreign_table) -> TableRelationship:
        if isinstance(foreign_table, str):
            foreign_table_name = foreign_table
        elif isinstance(foreign_table, Table) or isinstance(foreign_table, TableInfo):
            foreign_table_name = foreign_table.name
        else:
            raise Exception(f"Unexpected type {type(foreign_table)} for foreign_table. Only expecting str, Table, or TableInfo")
        if foreign_table_name not in self.relationship_map.keys():
            raise RelationshipNotFound(f"Relationship not found between {self.name} and {foreign_table_name}")
        return self.relationship_map[foreign_table_name]
    
    def get_data_column_infos(self):
        data_column_infos = [column_info for column_info in self.column_infos if column_info.data_returns]
        data_column_infos.extend([column_info for column_info in self.virtual_column_infos if column_info.data_returns])
        return data_column_infos

    def get_data_db_columns(self):
        data_db_columns = [column_info.db_column for column_info in self.column_infos if column_info.data_returns]
        data_db_columns.extend([column_info.db_column for column_info in self.virtual_column_infos if column_info.data_returns])
        return data_db_columns
    
    def get_summary_column_infos(self):
        data_column_infos = [column_info for column_info in self.column_infos if column_info.summary_returns]
        data_column_infos.extend([column_info for column_info in self.virtual_column_infos if column_info.summary_returns])
        return data_column_infos

    def get_summary_db_columns(self):
        data_db_columns = [column_info.db_column for column_info in self.column_infos if column_info.summary_returns]
        data_db_columns.extend([column_info.db_column for column_info in self.virtual_column_infos if column_info.summary_returns])
        return data_db_columns
    
    def get_summary_process_before_display_column_infos(self):
        data_db_columns = [column_info for column_info in self.column_infos if column_info.process_before_display is not None]
        data_db_columns.extend([column_info for column_info in self.virtual_column_infos if column_info.process_before_display is not None])
        return data_db_columns
    
    def get_summary_process_before_display_db_columns(self):
        data_db_columns = [column_info.db_column for column_info in self.column_infos if column_info.process_before_display is not None]
        data_db_columns.extend([column_info.db_column for column_info in self.virtual_column_infos if column_info.process_before_display is not None])
        return data_db_columns
    
    def get_column_infos(self, typ):
        if typ == 'summary':
            return self.get_summary_column_infos()
        else:
            return self.get_data_column_infos()