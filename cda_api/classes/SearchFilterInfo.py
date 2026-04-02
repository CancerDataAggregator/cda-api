from cda_api.classes.DatabaseInfo import DatabaseInfo
from cda_api.classes.TableRelationship import TableRelationship
from cda_api.db.filter_functions import case_insensitive_equals, case_insensitive_like
from sqlalchemy.orm import Session
from sqlalchemy import func, intersect, union
from cda_api.db.query_functions import list_to_tsquery, validate_tsquery, get_cte_column, print_query
from cda_api import InvalidSearchError
import time
import re

class SearchFilterInfo:
    def __init__(self, db: Session, search_list, db_info: DatabaseInfo, endpoint_table_info, log):
        self.db = db
        self.search_list = list(set([' '.join([word.replace('*', '%').lower() for word in keyword.split()]) for keyword in search_list]))
        self.db_info = db_info
        self.endpoint_table_info = endpoint_table_info
        self.log = log

        # Set useful variables
        self.endpoint_alias = self.endpoint_table_info.primary_key_column_info
        self.endpoint_unique_id = f'{self.endpoint_table_info.name}_alias'

        self.other_local_table_info = [local_table_info for local_table_info in self.db_info.local_table_infos if local_table_info != self.endpoint_table_info][0]

        self.local_table_relationship = self.endpoint_table_info.get_table_relationship(self.other_local_table_info)
        self.local_mapping_column = self.local_table_relationship.local_mapping_column_info.db_column.label(self.endpoint_unique_id)
        self.foreign_mapping_column = self.local_table_relationship.foreign_mapping_column_info.db_column

        self.local_table_infos = [self.endpoint_table_info, self.other_local_table_info]
        
        self._process_keywords()
        self._build_search_preselect_cte()

    def __repr__(self):
        repr_str = 'SearchFilterInfo(\n'
        for table_info, keyword_query_map in self.exclusive_keyword_cte_map.items():
            repr_str += f'\n\t{table_info}) exclusive matched keywords: {list(keyword_query_map.keys())}'
        repr_str += f'\n\t Commonly matched keywords: {list(self.common_keyword_query_map.keys())}'
        if validate_tsquery(self.db, self.ts_query):
            repr_str += f"\n\t\tUnmatched Keyword ts_vector input: to_tsquery('english', '{' & '.join(self.unmatched_keywords)}')"
        else:
            repr_str += f"\n\t\tUnmatched Keyword ts_vector input: NONE"
        
        repr_str += f"\n)"
        return repr_str
    
    def _get_keyword_table_info_and_relationship(self, local_table_info):
        keyword_table_info = self.db_info.get_table_info(f'{local_table_info.name}_keywords')
        keyword_relationship = self.db_info.get_table_relationship(local_table_info, keyword_table_info)
        return keyword_table_info, keyword_relationship

    def _get_text_search_table_info_and_relationship(self, local_table_info):
        text_search_table_info = self.db_info.get_table_info(f'{local_table_info.name}_text_search')
        text_search_relationship = self.db_info.get_table_relationship(local_table_info, text_search_table_info)
        return text_search_table_info, text_search_relationship

    def _get_keyword_cte_column(self, local_table_info, keyword, index):
        keyword_table_info, _ = self._get_keyword_table_info_and_relationship(local_table_info)
        keyword_column = keyword_table_info.get_column_info('keyword').db_column
        keyword_id_column = keyword_table_info.primary_key_column_info.db_column

        # Check if keyword exists
        if '%' in keyword:
            search_table_keyword_filter = case_insensitive_like(keyword_column, keyword)
        else:
            search_table_keyword_filter = case_insensitive_equals(keyword_column, keyword)
        keyword_query = self.db.query(keyword_id_column).filter(search_table_keyword_filter)
        if not keyword_query.count():
            # Does not exist
            return None
        
        # Construct keyword CTE
        cte_alias_prefix = re.sub(r'[^a-zA-Z0-9_]', '', '_'.join(keyword.split())).lower()
        keyword_query_cte = keyword_query.cte(f'{local_table_info.name}_{cte_alias_prefix}_{index}_keyword_ids_preselect')
        return keyword_query_cte.c[0]

    def _get_endpoint_id_keyword_query(self, local_table_info, keyword_cte_columns):
        _, keyword_relationship = self._get_keyword_table_info_and_relationship(local_table_info)
        keyword_filter = self.db.query(keyword_relationship.local_mapping_column_info.db_column.label(self.endpoint_unique_id))
        for keyword_cte_column in keyword_cte_columns:
            keyword_filter = keyword_filter.filter(keyword_relationship.foreign_mapping_column_info.db_column.in_(self.db.query(keyword_cte_column)))

        if local_table_info != self.endpoint_table_info:
            keyword_filter = self.db.query(self.local_mapping_column.label(self.endpoint_unique_id)).filter(self.foreign_mapping_column.in_(keyword_filter))
        
        return keyword_filter

    def _process_keywords(self):
        start_time = time.time()
        self.unmatched_keywords = []
        self.common_keyword_query_map = {}
        self.exclusive_keyword_cte_map = {local_table_info: {} for local_table_info in self.local_table_infos}
        for index, keyword in enumerate(self.search_list):
            endpoint_keyword_cte_column = self._get_keyword_cte_column(self.endpoint_table_info, keyword, index)
            other_keyword_cte_column = self._get_keyword_cte_column(self.other_local_table_info, keyword, index)

            # Get unmatched keywords
            if endpoint_keyword_cte_column is None and other_keyword_cte_column is None:
                if len(keyword.split()) > 1:
                    msg = f'Phrase: "{keyword}" yielded no results'
                    self.log.error(msg)
                    raise InvalidSearchError(msg)
                if '%' in keyword:
                    msg = f'Term with wildcard: "{keyword.replace('%', '*')}" yielded no results'
                    self.log.error(msg)
                    raise InvalidSearchError(msg)
                # Test search_query
                self.unmatched_keywords.append(keyword)
            
            # Get common keywords
            elif endpoint_keyword_cte_column is not None and other_keyword_cte_column is not None:
                self.common_keyword_query_map[keyword] = union(self._get_endpoint_id_keyword_query(self.endpoint_table_info, [endpoint_keyword_cte_column]), 
                                                               self._get_endpoint_id_keyword_query(self.other_local_table_info, [other_keyword_cte_column]))
            
            # Get table exclusive keywords
            elif endpoint_keyword_cte_column is not None:
                self.exclusive_keyword_cte_map[self.endpoint_table_info][keyword] = endpoint_keyword_cte_column
            
            elif other_keyword_cte_column is not None:
                self.exclusive_keyword_cte_map[self.other_local_table_info][keyword] = other_keyword_cte_column
            
            else:
                raise InvalidSearchError('Issue with processing search')
        query_time = time.time() - start_time
        self.log.info(f"Keyword processing time: {query_time}s")
        
    
    def _build_search_preselect_cte(self):
        intersection_filter_list = []

        # Get exclusive filters
        for local_table_info, keyword_filter_query_map in self.exclusive_keyword_cte_map.items():
            exclusive_endpoint_filters = [self._get_endpoint_id_keyword_query(local_table_info, [filter_query]) for _, filter_query in keyword_filter_query_map.items()]
            if exclusive_endpoint_filters:
                exclusive_cte = intersect(*exclusive_endpoint_filters).cte(f'{local_table_info.name}_exclusive_keywords_preselect')
                intersection_filter_list.append(self.db.query(exclusive_cte.c[0].label(self.endpoint_unique_id)))
        
        # Get common filters
        common_filters = [self.db.query(common_union.c[0]) for _, common_union in self.common_keyword_query_map.items()]
        # common_filters = [self.db.query().from_statement(self.db.query(common_union.c[0])) for _, common_union in self.common_keyword_query_map.items()]
        if common_filters:
            common_cte = intersect(*common_filters).cte('unified_keyword_preselect')
            intersection_filter_list.append(self.db.query(common_cte.c[0].label(self.endpoint_unique_id)))
        
        # Get text search filters
        self.ts_query = list_to_tsquery(self.unmatched_keywords)
        if validate_tsquery(self.db, self.ts_query):
            text_search_filters = []
            for local_table_info in self.local_table_infos:
                local_unique_id = f'{local_table_info.name}_alias'
                text_search_table_info, text_search_relationship = self._get_text_search_table_info_and_relationship(local_table_info)
                text_vector_subquery = self.db.query(text_search_relationship.foreign_column_info.db_column.label(local_unique_id))\
                                            .filter(text_search_table_info.get_column_info('search_vector').db_column.op('@@')(self.ts_query))
                if local_table_info != self.endpoint_table_info:
                    text_vector_subquery = self.db.query(self.local_mapping_column.label(self.endpoint_unique_id))\
                                            .filter(self.foreign_mapping_column.in_(text_vector_subquery))
                text_search_filters.append(text_vector_subquery)
            text_search_union_cte = union(*text_search_filters).cte('unmatched_keyword_text_search_preselect')
            intersection_filter_list.append(self.db.query(text_search_union_cte.c[0].label(self.endpoint_unique_id)))
        
        self.search_preselect_cte = intersect(*[filter_query.subquery().select() for filter_query in intersection_filter_list]).cte('search_preselect')
        
        


    def get_filterable_preselect(self, filter_preselect_map):
        endpoint_preselect_db_column = filter_preselect_map[self.endpoint_table_info].db_column
        filterable_preselect = endpoint_preselect_db_column.in_(self.db.query(get_cte_column(self.search_preselect_cte , self.endpoint_unique_id)))
        return filterable_preselect