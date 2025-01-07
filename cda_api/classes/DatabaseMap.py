from cda_api import ColumnNotFound, RelationshipNotFound, TableNotFound, get_logger
from .ColumnInfo import ColumnInfo
from .EntityRelationship import EntityRelationship
from sqlalchemy import inspect, Column, func
from cda_api.db.schema import Base
from cda_api.db.connection import session

setup_log = get_logger('Setup: DatabaseMap.py')
log = get_logger('Util: DatabaseMap.py')

# Build Column Metadata Map
setup_log.info('Building column_metadata map')

column_metadata = Base.metadata.tables['column_metadata']
subquery = session().query(column_metadata).subquery('json_result')
query = session().query(func.row_to_json(subquery.table_valued()))
result = query.all()
result = [row for row, in result]

COLUMN_METADATA_MAP = {}
for row in result:
    tablename = row['cda_table']
    columnname = row['cda_column']
    metadata = {k:v for k,v in row.items() if k not in ['cda_table', 'cda_column']}
    if tablename not in COLUMN_METADATA_MAP.keys():
        COLUMN_METADATA_MAP[tablename] = {}
    if columnname not in COLUMN_METADATA_MAP[tablename].keys():
        COLUMN_METADATA_MAP[tablename][columnname] = metadata

class DatabaseMap():
    def __init__(self, db_base):
        setup_log.info('Building DatabaseMap Object')
        self.db_base = db_base
        self._build_metadata_variables()
        self._build_entity_table_variables()
        self._build_column_map()
        self._build_relationship_map()
        self._build_file_dicom_column_map()
        self._build_hanging_table_relationship_map()

    def _build_metadata_variables(self):
        setup_log.info('Building metadata variables from automapped Base')
        self.metadata_tables = self.db_base.metadata.tables
        self.metadata_columns = [column for table in self.metadata_tables.values()
                                        for column in table.columns]
        self.metadata_column_names = [column.name for column in self.metadata_columns]

    def _build_entity_table_variables(self):
        setup_log.info('Building entity class variables from automapped Base')
        self.entity_tables = self.db_base.classes
        self.entity_tablenames = self.entity_tables.keys()
        self.entity_columnnames = [column.name for column in self.metadata_columns 
                                   if column.table.name in self.entity_tablenames]
        self.entity_table_column_map = {tablename: self.metadata_tables[tablename].columns.values()
                                        for tablename in self.entity_tablenames} 

    def _build_column_map(self):
        setup_log.info('Building column map')
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
                
                self.column_map[uniquename] = ColumnInfo(uniquename=uniquename,
                                                         entity_table=entity_table, 
                                                         metadata_table=metadata_table, 
                                                         metadata_column=metadata_column,
                                                         column_metadata_map=COLUMN_METADATA_MAP)
                
    def _build_relationship_map(self):
        setup_log.info('Building relationship map')
        self.relationship_tablenames = set()
        self.relationship_map = {}
        for tablename, table in self.entity_tables.items():
            self.relationship_map[tablename] = {}
            i = inspect(table)
            for relationship in i.relationships:
                if tablename != relationship.target.name:
                    self.relationship_map[tablename][relationship.target.name] = EntityRelationship(tablename, relationship)
                    try: # No Boolean clause defined for recognizing if relationship.secondary is None, hence passing on the AttributeError
                        self.relationship_tablenames.add(str(relationship.secondary.name))
                    except AttributeError as e:
                        pass
                    except Exception as e:
                        raise e
                    
    def _build_file_dicom_column_map(self):
        self.file_dicom_column_map = {
            'file_id'                               : self.get_column_info('dicom_series_instance_id'),
            'file_id_alias'                         : self.get_column_info('dicom_series_id_alias'),
            'file_crdc_id'                          : self.get_column_info('dicom_series_instance_crdc_id'),
            'file_name'                             : self.get_column_info('dicom_series_instance_name'),
            'file_description'                      : self.get_column_info('dicom_series_description'),
            'file_drs_uri'                          : self.get_column_info('dicom_series_instance_drs_uri'),
            'file_access'                           : self.get_column_info('dicom_series_access'),
            'file_size'                             : self.get_column_info('dicom_series_instance_size'),
            'file_checksum_type'                    : self.get_column_info('dicom_series_checksum_type'),
            'file_checksum_value'                   : self.get_column_info('dicom_series_instance_checksum_value'),
            'file_format'                           : self.get_column_info('dicom_series_format'),
            'file_type'                             : self.get_column_info('dicom_series_instance_type'),
            'file_category'                         : self.get_column_info('dicom_series_category'),
            'file_data_at_cds'                      : self.get_column_info('dicom_series_data_at_cds'),
            'file_data_at_gdc'                      : self.get_column_info('dicom_series_data_at_gdc'),
            'file_data_at_icdc'                     : self.get_column_info('dicom_series_data_at_icdc'),
            'file_data_at_idc'                      : self.get_column_info('dicom_series_data_at_idc'),
            'file_data_at_pdc'                      : self.get_column_info('dicom_series_data_at_pdc'),
            'file_data_source_count'                : self.get_column_info('dicom_series_data_source_count'),
            'file_anatomic_site_anatomic_site'      : self.get_column_info('dicom_series_anatomic_site_anatomic_site'),
            'file_tumor_vs_normal_tumor_vs_normal'  : self.get_column_info('dicom_series_tumor_vs_normal_tumor_vs_normal')
        }

    
    def _build_hanging_table_relationship_map(self):
        excluded_tables = ['project_in_project', 'release_metadata', 'column_metadata', 'upstream_identifiers']
        hanging_table_dict = { tablename: table for tablename, table in  Base.metadata.tables.items() 
                            if (tablename not in self.entity_tablenames) and 
                                (tablename not in self.relationship_tablenames) and 
                                (tablename not in excluded_tables) }

        self.hanging_table_relationship_map = {tablename: {} for tablename in hanging_table_dict.keys()}

        for hanging_tablename, hanging_table in hanging_table_dict.items():
            fk = list(hanging_table.foreign_keys)[0]
            hanging_table_alias = fk.parent
            connecting_table_alias = fk.column
            connecting_table = fk.column.table
            connecting_tablename = connecting_table.name
            
            for entity_tablename in ['subject', 'file']:
                if connecting_tablename == entity_tablename:
                    self.hanging_table_relationship_map[hanging_tablename][entity_tablename] = {'join_table': hanging_table, 
                                                                                                'statement': hanging_table_alias == connecting_table_alias}

                elif connecting_tablename == 'dicom_series' and entity_tablename == 'file':
                        pass
                else:
                    entity_connecting_relationship = self.get_relationship(entity_tablename=entity_tablename, foreign_tablename=connecting_tablename)
                    mapping_table_to_hanging_table_join = {'join_table': entity_connecting_relationship.mapping_table, 
                                                           'statement': entity_connecting_relationship.foreign_mapping_column == hanging_table_alias, 
                                                           'hanging_fk_parent': hanging_table_alias,
                                                           'entity_mapping_columnname': entity_connecting_relationship.entity_mapping_column.name,
                                                           'entity_mapping_column': entity_connecting_relationship.entity_mapping_column,
                                                           'entity_column': entity_connecting_relationship.entity_column, 
                                                           'entity_mapping_join': entity_connecting_relationship.entity_column == entity_connecting_relationship.entity_mapping_column}
                    self.hanging_table_relationship_map[hanging_tablename][entity_tablename] = mapping_table_to_hanging_table_join

            if connecting_tablename == 'dicom_series':
                self.hanging_table_relationship_map[hanging_tablename]['dicom_series'] = {'join_table': hanging_table, 
                                                                                          'statement': hanging_table_alias == connecting_table_alias}
        

    def get_column_info(self, columnname) -> ColumnInfo:
        try: 
            return self.column_map[columnname]
        except Exception as e:
            possible_cols = [k for k in self.column_map.keys() if k.endswith(columnname)]
            possible_cols.extend([k for k in self.column_map.keys() if k.startswith(columnname)])
            if possible_cols:
                error_message = f'Column Not Found: {columnname}, did you mean: {possible_cols}\n{e}'
            else:
                error_message = f'Column Not Found: {columnname}\n{e}'
            raise ColumnNotFound(error_message)
        
    def get_meta_column(self, columnname) -> Column:
        try: 
            return self.column_map[columnname].metadata_column
        except Exception as e:
            possible_cols = [k for k in self.column_map.keys() if k.endswith(columnname)]
            possible_cols.extend([k for k in self.column_map.keys() if k.startswith(columnname)])
            if possible_cols:
                error_message = f'Column Not Found: {columnname}, did you mean: {possible_cols}\n{e}'
            else:
                error_message = f'Column Not Found: {columnname}\n{e}'
            raise ColumnNotFound(error_message)

    def get_relationship(self, entity_tablename, foreign_tablename) -> EntityRelationship:
        try:
            return self.relationship_map[entity_tablename][foreign_tablename]
        except Exception as e:
            error_message = f'Unable to find relationship between {entity_tablename} and {foreign_tablename}\n{e}'
            raise RelationshipNotFound(error_message)
    
    def get_hanging_table_join(self, hanging_tablename, entity_tablename):
        try:
            return self.hanging_table_relationship_map[hanging_tablename][entity_tablename]
        except Exception as e:
            error_message = f'Unable to find relationship between hanging table {hanging_tablename} and {entity_tablename}\n{e}'
            raise RelationshipNotFound(error_message)
        
    def get_entity_table(self, entity_tablename):
        try:
            return self.entity_tables[entity_tablename]
        except Exception as e:
            error_message = f'Unable to find entity table {entity_tablename}\n{e}'
            raise TableNotFound(error_message)
        
    def get_metadata_table(self, tablename):
        try:
            return self.metadata_tables[tablename]
        except Exception as e:
            error_message = f'Unable to find entity table {tablename}\n{e}'
            raise TableNotFound(error_message)
        
    def get_metadata_table_columns(self, tablename):
        try:
            return self.metadata_tables[tablename].columns.values()
        except Exception as e:
            error_message = f'Unable to find entity table {tablename}\n{e}'
            raise TableNotFound(error_message)

    def get_table_column_infos(self, tablename):
        try:
            return [column_info for column_info in self.column_map.values() if column_info.tablename == tablename]
        except ColumnNotFound as cnf:
            raise cnf
        except Exception as e:
            error_message = f'Unable to find entity table {tablename}\n{e}'
            raise TableNotFound(error_message)
        
    def get_uniquename_metadata_table_columns(self, tablename):
        try:
            column_infos = self.get_table_column_infos(tablename)
            return [column_info.metadata_column.label(column_info.uniquename) for column_info in column_infos]
        except Exception as e:
            error_message = f'Unable to find entity table {tablename}\n{e}'
            raise TableNotFound(error_message)

    def get_column_uniquename(self, columnname, tablename):
        try:
            column_infos = self.get_table_column_infos(tablename)
            uniquename = [column_info.uniquename for column_info in column_infos if column_info.columnname == columnname][0]
            if not uniquename:
                error_message = f'Unable to get unique name for "{columnname}" in {tablename}'
                raise ColumnNotFound(error_message)
            return uniquename
        except ColumnNotFound as cnf:
            raise cnf
        except TableNotFound as tnf:
            raise tnf
        except Exception as e:
            raise TableNotFound(e)
        