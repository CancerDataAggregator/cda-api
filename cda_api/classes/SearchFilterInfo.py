from cda_api.classes.DatabaseInfo import DatabaseInfo
from cda_api.db.filter_functions import case_insensitive_equals, case_insensitive_like
from sqlalchemy.orm import Session
from sqlalchemy import func, intersect
from cda_api.db.query_functions import list_to_tsquery, get_cte_column
import re

class SearchFilterInfo:
    def __init__(self, db: Session, search_string, db_info: DatabaseInfo, endpoint_table_info, log):
        self.db = db
        self.search_string = search_string
        self.search_list = [keyword.replace('*', '%') for keyword in self.search_string.split()]
        self.db_info = db_info
        self.endpoint_table_info = endpoint_table_info
        self.log = log

        # Set useful variables
        self.endpoint_alias = self.endpoint_table_info.primary_key_column_info
        self.endpoint_unique_id = f'{self.endpoint_table_info.name}_alias'

        self.keyword_table_info = self.db_info.get_table_info(f'{self.endpoint_table_info.name}_keywords')
        self.keyword_relationship = self.db_info.get_table_relationship(self.endpoint_table_info, self.keyword_table_info)

        self.text_search_table_info = self.db_info.get_table_info(f'{self.endpoint_table_info.name}_text_search')
        self.text_search_relationship = self.db_info.get_table_relationship(self.endpoint_table_info, self.text_search_table_info)
        
        self._set_matching_keyword_query_map()
        self._build_search_preselect_cte()

    def __repr__(self):
        repr_components = [
            f"SearchFilterInfo(",
            f"\tMatched Keywords: {list(self.matched_keyword_query_map)}",
            f"\tUnmatched Keyword ts_vector input: to_tsquery('english', {' & '.join([keyword.lower().replace('%','') for keyword in self.unmatched_keywords])}')"
            f")"
        ]
        return '\n'.join(repr_components)
    
    def _set_matching_keyword_query_map(self):
        keyword_column = self.keyword_table_info.get_column_info('keyword').db_column
        keyword_id_column = self.keyword_table_info.primary_key_column_info.db_column
        self.keyword_query_map = {keyword: None for keyword in self.search_list}
        for keyword in self.search_list:
            if '%' in keyword:
                search_table_keyword_filter = case_insensitive_like(keyword_column, keyword)
            else:
                search_table_keyword_filter = case_insensitive_equals(keyword_column, keyword)
            keyword_query = self.db.query(keyword_id_column).filter(search_table_keyword_filter)
            result = keyword_query.count()
            if result > 0:
                self.keyword_query_map[keyword] = keyword_query
    
    def _build_search_preselect_cte(self):
        filter_query_list = []
        # TODO: Ask Arthur if we should just let the query potentially find results from a keyword that wasn't found in the keywords table but could be found without the wildcard?
        self.unmatched_keywords = [keyword for keyword, query in self.keyword_query_map.items() if query is None]
        self.matched_keyword_query_map = {keyword:query for keyword, query in self.keyword_query_map.items() if query is not None}

        i = 0
        # Create filters for matching keywords directly to the *_keywords table
        for keyword, keyword_query in self.matched_keyword_query_map.items():
            keyword_filter = self.db.query(self.keyword_relationship.local_mapping_column_info.db_column.label(self.endpoint_unique_id))

            # If we only have one keyword and no vector search to perform, then there is no reason to create both a keyword CTE and the search_preselect CTE
            if len(self.matched_keyword_query_map.keys()) == 1 and len(self.unmatched_keywords) == 0:
                keyword_filter = keyword_filter.filter(self.keyword_relationship.foreign_mapping_column_info.db_column.in_(keyword_query))
                self.search_preselect_cte = keyword_filter.cte(f'search_preselect')
                return
            
            cte_alias_prefix = re.sub(r'[^a-zA-Z0-9]', '', keyword).lower()

            keyword_query_cte = keyword_query.cte(f'keyword_{cte_alias_prefix}_{i}_ids_preselect')
            keyword_filter = keyword_filter.filter(self.keyword_relationship.foreign_mapping_column_info.db_column.in_(self.db.query(keyword_query_cte.c[0])))
            
            filter_query_list.append(keyword_filter)
            i+=1

        # Add text search vector subquery
        if self.unmatched_keywords:
            text_vector_subquery = self.db.query(self.text_search_relationship.foreign_column_info.db_column.label(self.endpoint_unique_id))\
                                          .filter(self.text_search_table_info.get_column_info('search_vector').db_column.op('@@')(list_to_tsquery(self.unmatched_keywords)))
            # If we only have no matching keywords then use just the text search for the search_preselect CTE
            if not self.matched_keyword_query_map:
                self.search_preselect_cte = text_vector_subquery.cte(f'search_preselect')
                return

            filter_query_list.append(text_vector_subquery)
        
        # Intersect the multiple filters
        self.search_preselect_cte = intersect(*filter_query_list).cte('search_preselect')


    def get_filterable_preselect(self, filter_preselect_map):
        endpoint_preselect_db_column = filter_preselect_map[self.endpoint_table_info].db_column
        filterable_preselect = endpoint_preselect_db_column.in_(self.db.query(get_cte_column(self.search_preselect_cte , self.endpoint_unique_id)))
        return filterable_preselect