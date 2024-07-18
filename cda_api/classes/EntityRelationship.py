from cda_api import RelationshipError
from cda_api import get_logger

log = get_logger()

class EntityRelationship():
    def __init__(self, entity_tablename, relationship):
        self.entity_tablename = entity_tablename
        self.foreign_tablename = relationship.target.name
        self.relationship = relationship
        self.entity_collection = relationship.class_attribute
        self.entity_collection_name = relationship.key
        self._build_mapping_columns()
    
    def __repr__(self):
        return f"entity_mapping_column: {self.entity_mapping_column}, foreign_mapping_column: {self.foreign_mapping_column}"
    
    def _build_mapping_columns(self):
        mapping_table_columns = self.relationship.remote_side
        if len(mapping_table_columns) < 2:
            raise RelationshipError(f'Mapping between {self.entity_tablename} and {self.foreign_tablename} not found')
        self.mapping_table = list(mapping_table_columns)[0].table
        try: 
            for column in mapping_table_columns:
                if len(column.foreign_keys) != 1:
                    raise RelationshipError(f'Individual foreign key for {column.name} not found when mapping between {self.entity_tablename} and {self.foreign_tablename} not found')
                foreign_key = list(column.foreign_keys)[0]
                foreign_tablename = foreign_key.column.table.name
                if foreign_tablename == self.foreign_tablename:
                    self.foreign_mapping_column = column
                    self.foreign_column = foreign_key.column
                else:
                    self.entity_mapping_column = column
                    self.entity_column = foreign_key.column
        except:
            log.exception(f"Error encountered while trying to build relationship between {self.entity_tablename} and {self.foreign_tablename}")
            raise RelationshipError