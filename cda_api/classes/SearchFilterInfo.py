from cda_api.classes.DatabaseInfo import DatabaseInfo
from cda_api.classes.TableRelationship import TableRelationship
from cda_api.db.filter_functions import case_insensitive_equals, case_insensitive_like
from sqlalchemy.orm import Session
from sqlalchemy import func, intersect, union
from cda_api.db.query_functions import list_to_tsquery, get_cte_column, print_query
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

        self.other_local_table_info = [local_table_info for local_table_info in self.db_info.local_table_infos if local_table_info != self.endpoint_table_info][0]
        
        self._set_matching_keyword_query_map()
        self._build_search_preselect_cte()

    def __repr__(self):
        repr_str = 'SearchFilterInfo(\n'
        for tablename, keyword_query_map in self.keyword_query_map.items():
            repr_str += f'\n\tTableInfo({tablename}):'
            matched_keywords = [keyword for keyword, query in keyword_query_map.items() if query is not None]
            unmatched_keywords = [keyword for keyword, query in keyword_query_map.items() if query is None]
            repr_str += f"\n\t\tMatched Keywords: {matched_keywords}"

            repr_str += f"\n\t\tUnmatched Keyword ts_vector input: to_tsquery('english', {' & '.join([keyword.lower().replace('%','') for keyword in unmatched_keywords])}')"
        
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

    
    def _set_matching_keyword_query_map(self):
        self.keyword_query_map = {table_info.name: None for table_info in self.db_info.local_table_infos}
        for local_table_info in self.db_info.local_table_infos:
            self.keyword_query_map[local_table_info.name] = {keyword: None for keyword in self.search_list}
            keyword_table_info, _ = self._get_keyword_table_info_and_relationship(local_table_info)
            keyword_column = keyword_table_info.get_column_info('keyword').db_column
            keyword_id_column = keyword_table_info.primary_key_column_info.db_column
            for keyword in self.search_list:
                if '%' in keyword:
                    search_table_keyword_filter = case_insensitive_like(keyword_column, keyword)
                else:
                    search_table_keyword_filter = case_insensitive_equals(keyword_column, keyword)
                keyword_query = self.db.query(keyword_id_column).filter(search_table_keyword_filter)
                result = keyword_query.count()
                if result > 0:
                    self.keyword_query_map[local_table_info.name][keyword] = keyword_query
        
    
    def _build_search_preselect_cte(self):
        local_search_preselect_map = {}
        for local_table_info in self.db_info.local_table_infos:
            local_search_preselect_alias = f'{local_table_info.name}_search_preselect'
            local_unique_id = f'{local_table_info.name}_alias'
            filter_query_list = []
            # TODO: Ask Arthur if we should just let the query potentially find results from a keyword that wasn't found in the keywords table but could be found without the wildcard?
            unmatched_keywords = [keyword for keyword, query in self.keyword_query_map[local_table_info.name].items() if query is None]
            matched_keyword_query_map = {keyword:query for keyword, query in self.keyword_query_map[local_table_info.name].items() if query is not None}
            _, keyword_relationship = self._get_keyword_table_info_and_relationship(local_table_info)
            text_search_table_info, text_search_relationship = self._get_text_search_table_info_and_relationship(local_table_info)

            i = 0
            # Create filters for matching keywords directly to the *_keywords table
            for keyword, keyword_query in matched_keyword_query_map.items():
                keyword_filter = self.db.query(keyword_relationship.local_mapping_column_info.db_column.label(local_unique_id))

                # If we only have one keyword and no vector search to perform, then there is no reason to create both a keyword CTE and the search_preselect CTE
                if len(matched_keyword_query_map.keys()) == 1 and len(unmatched_keywords) == 0:
                    keyword_filter = keyword_filter.filter(keyword_relationship.foreign_mapping_column_info.db_column.in_(keyword_query))
                    local_search_preselect_map[local_table_info] = keyword_filter.cte(local_search_preselect_alias)
                    
                else:
                    cte_alias_prefix = re.sub(r'[^a-zA-Z0-9]', '', keyword).lower()
                    keyword_query_cte = keyword_query.cte(f'{local_table_info.name}_keyword_{cte_alias_prefix}_{i}_ids_preselect')
                    keyword_filter = keyword_filter.filter(keyword_relationship.foreign_mapping_column_info.db_column.in_(self.db.query(keyword_query_cte.c[0])))
                    filter_query_list.append(keyword_filter)
                    i+=1

            # Add text search vector subquery
            if unmatched_keywords:
                text_vector_subquery = self.db.query(text_search_relationship.foreign_column_info.db_column.label(local_unique_id))\
                                            .filter(text_search_table_info.get_column_info('search_vector').db_column.op('@@')(list_to_tsquery(unmatched_keywords)))
                # If we only have no matching keywords then use just the text search for the search_preselect CTE
                if not matched_keyword_query_map:
                    local_search_preselect_map[local_table_info] = text_vector_subquery.cte(local_search_preselect_alias)
                else:
                    filter_query_list.append(text_vector_subquery)
            
            # Intersect the multiple filters
            if len(filter_query_list) > 1:
                local_search_preselect_map[local_table_info] = intersect(*filter_query_list).cte(local_search_preselect_alias)

        endpoint_preselect = local_search_preselect_map[self.endpoint_table_info]
        endpoint_subquery = self.db.query(get_cte_column(endpoint_preselect, self.endpoint_unique_id).label(self.endpoint_unique_id))

        other_local_preselect = local_search_preselect_map[self.other_local_table_info]
        local_table_relationship = self.endpoint_table_info.get_table_relationship(self.other_local_table_info)
        other_table_subquery = self.db.query(local_table_relationship.local_mapping_column_info.db_column.label(self.endpoint_unique_id))\
                                      .filter(local_table_relationship.foreign_mapping_column_info.db_column.in_(self.db.query(other_local_preselect.c)))

        self.search_preselect_cte = union(endpoint_subquery, other_table_subquery).cte('search_preselect')
        
        


    def get_filterable_preselect(self, filter_preselect_map):
        endpoint_preselect_db_column = filter_preselect_map[self.endpoint_table_info].db_column
        filterable_preselect = endpoint_preselect_db_column.in_(self.db.query(get_cte_column(self.search_preselect_cte , self.endpoint_unique_id)))
        return filterable_preselect