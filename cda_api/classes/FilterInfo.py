from sqlalchemy.sql import select, exists
from .DatabaseInfo import DatabaseInfo
from cda_api import RelationshipError
from cda_api.db.filter_functions import parse_filter_string, apply_filter_operator

class FilterInfo:
    def __init__(self, filter_string, filter_type, db_info: DatabaseInfo, log):
        self.filter_string = filter_string
        if filter_type not in ['match_all', 'match_some']:
            raise Exception(f'Filter type: {filter_type} not recognized please select from ["match_all", "match_some"]')
        self.filter_type = filter_type
        self.db_info = db_info
        self.log = log
        self.build_filter_components()
    
    def __repr__(self):
        repr_components = [
            f'FilterInfo({self.filter_column_info.name} {self.filter_operator} {self.filter_value})'
        ]
        return '\n'.join(repr_components)
    
    def build_filter_components(self):
        self.filter_column_name, self.filter_operator, self.filter_value = parse_filter_string(self.filter_string, self.log)
        self.filter_column_info = self.db_info.get_column_info(self.filter_column_name)
        self.selectable_column_info = self.filter_column_info
        self.local_filter_clause = apply_filter_operator(self.filter_column_info.db_column, self.filter_value, self.filter_operator, self.log)
        self.exclusively_null = False

        # Override if exclusively null
        if (self.filter_operator == 'is') and (self.filter_value is None):
            self.exclusively_null = True
            if self.filter_column_info.parent_table_info.name == "project":
                raise RelationshipError(f'Cannot properly filter "project" columns as being null: "{self.filter_column_info.name} =/is/== null" is not valid')
            if (self.filter_column_name in ['tumor_vs_normal', 'anatomic_site']):
                self.filter_column_info = self.filter_column_info.parent_table_info.database_info.get_column_info('file_alias', f'{self.filter_column_info.parent_table_info.name}_nulls')
                self.filter_operator = 'exists'
                self.filter_value = ''
                self.local_filter_clause = None
            elif (self.filter_column_info.null_column_info is not None):
                self.filter_column_info = self.filter_column_info.null_column_info
                self.filter_operator = 'is'
                self.filter_value  = 'True'
                self.local_filter_clause = self.filter_column_info.db_column.is_(True)
            else:
                self.log.warning(f'Could not build exclusive null filter for {self}')
                self.exclusively_null = False
        
    
    def get_filterable_preselect(self, filter_preselect_map, endpoint_table_info):
        filter_table_info = self.filter_column_info.parent_table_info
        filterable_table_info = filter_table_info.primary_table_info
        if filterable_table_info not in filter_preselect_map.keys():
            # Default to endpoint table for cases like upstream_idenfiers
            filterable_table_info = endpoint_table_info
        
        filterable_column_info = filter_preselect_map[filterable_table_info]

        self.log.debug(f'Getting filter {self}, for {filterable_table_info}')

        if filterable_table_info == filter_table_info:
            if len(filter_preselect_map.keys()) == 1:
                return self.local_filter_clause
            else:
                subquery = select(1).select_from(filter_table_info.db_table)\
                                    .filter(filterable_column_info.db_column == filter_table_info.primary_key_column_info.db_column)
        
        else:
            filter_table_relationship = filterable_table_info.get_table_relationship(filter_table_info)
            if filter_table_relationship.requires_mapping_table:
                subquery = select(1).select_from(filter_table_relationship.foreign_mapping_column_info.parent_table_info.db_table)\
                                    .filter(filterable_column_info.db_column == filter_table_relationship.local_mapping_column_info.db_column)\
                                    .filter(filter_table_relationship.foreign_mapping_column_info.db_column == filter_table_relationship.foreign_column_info.db_column)
            else:
                subquery = select(1).select_from(filter_table_relationship.foreign_column_info.parent_table_info.db_table)\
                                    .filter(filterable_column_info.db_column == filter_table_relationship.foreign_column_info.db_column)
            for additional_filter in filter_table_relationship.additional_filters:
                subquery = subquery.filter(additional_filter)
            
        if self.local_filter_clause is not None:
            subquery = subquery.filter(self.local_filter_clause)
        return exists(subquery)