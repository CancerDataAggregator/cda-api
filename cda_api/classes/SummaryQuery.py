from cda_api.db.query_functions import get_cte_column, column_distinct_count_subquery, foreign_table_distinct_count, data_source_counts, data_source_counts_cte, basic_categorical_summary, basic_categorical_summaries, null_aware_categorical_summary, null_aware_categorical_summaries, numeric_summary, numeric_summaries, get_selectable_db_column_and_possible_join
from .models import SummaryRequestBody
from .DatabaseInfo import DatabaseInfo
from .shared_class_functions import construct_search_filter_info, construct_filter_infos, get_table_column_and_filter_map, get_filtered_preselect
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import array 

class SummaryQuery:
    def __init__(self, db, db_info: DatabaseInfo, endpoint_table_name, request_body: SummaryRequestBody, log):
        # Initailize arguments
        self.db = db
        self.db_info = db_info
        self.endpoint_table_info = self.db_info.get_table_info(endpoint_table_name)
        self.request_body = request_body
        self.log = log
        self.log.info("Constructing SummaryQuery object")

        # Set useful variables
        self.endpoint_alias = self.endpoint_table_info.primary_key_column_info

        # Construct filter preselect
        self.search_filter_info = construct_search_filter_info(self)
        self.filter_infos = construct_filter_infos(self)
        self.table_column_and_filter_map = get_table_column_and_filter_map(self, 'summary')
        self.filtered_preselect, self.filtered_preselect_cte_query_map, self.filtered_preselect_column_map = get_filtered_preselect(self)

        # Build select query
        self._build_select_clause()
        self.log.debug("SummaryQuery object construction complete")
        

    def __repr__(self):
        table_column_filter_map_string = "\n".join([f"\t{table}\n\t\tcolumn_infos: {m['column_infos']}\n\t\tfilter_infos: {m['filter_infos']}" for table, m in self.table_column_and_filter_map.items()])
        select_map_string = "\n".join([f"\t{table_info}\n\t\t{'\n\t\t'.join([column.name for column in columns])}" for table_info, columns in self.select_map.items() ])
        select_columns_string_list = []
        for select_column in self.select_clause_columns:
            try:
                if select_column.table is not None:
                    col_name = f"{select_column.table.name}.{select_column.name}"
                else:
                    col_name = f"{select_column.element.table.name}.{select_column.name}"
            except:
                col_name = f"{select_column.name}"
            select_columns_string_list.append(col_name)
        select_columns_string ='[ ' + '\n'.join(select_columns_string_list) + ' ]'
        repr_components = [
            f'SummaryQuery({self.log.extra['id']})',
            f'Endpoint: {self.endpoint_table_info}', 
            f'SEARCH_STRING Filters:\n{self.search_filter_info}',
            f'MATCH_ALL Filters:\n{self.get_filter_infos('match_all')}',
            f'MATCH_SOME Filters:\n{self.get_filter_infos('match_some')}',
            f'Table Column and Filter Map:',
            f'{table_column_filter_map_string}',
            f'Select Map:',
            f'{select_map_string}',
            f'Ordered Select Columns',
            f'{select_columns_string}'
        ]
        return '\n'.join(repr_components)
    
    def _build_select_clause(self):
        self.log.debug("Constructing summary select clause")
        self.select_map = {'total_count': [], 'other_local_table_counts': []}
        self.select_clause_columns = []
        self.controlled_term_column_map = {}
        self._get_total_count()
        self._get_other_local_table_counts()
        self._get_column_summaries()

        total_count = []
        other_local_table_counts = []
        endpoint_column_summaries = []
        foreign_column_summaries = []

        for key, columns in self.select_map.items():
            if key == 'total_count':
                total_count.extend(columns)
            elif key == 'other_local_table_counts':
                other_local_table_counts.extend(columns)
            elif key == self.endpoint_table_info:
                endpoint_column_summaries.extend(columns)
            else:
                foreign_column_summaries.extend(columns)
        
        self.select_clause_columns = total_count + other_local_table_counts + endpoint_column_summaries + foreign_column_summaries

    
    def _get_total_count(self):
        self.log.debug(f"Constructing total_count select statement for {self.endpoint_table_info}")
        total_count = column_distinct_count_subquery(self.db, self.filtered_preselect_column_map[self.endpoint_table_info]).label('total_count')
        self.select_map['total_count'] = [total_count]


    def _get_other_local_table_counts(self):
        other_local_table_infos = [table_info for table_info in self.db_info.local_table_infos if table_info != self.endpoint_table_info]
        for other_local_table_info in other_local_table_infos:
            if other_local_table_info in self.filtered_preselect_column_map.keys():
                self.log.debug(f"Constructing count select statement for {other_local_table_info} directly from filtered preselect")
                distinct_count = column_distinct_count_subquery(self.db, self.filtered_preselect_column_map[other_local_table_info])
            else:
                self.log.debug(f"Constructing count select statement for {other_local_table_info} mapping from filtered preselect")
                distinct_count = foreign_table_distinct_count(self.db, self.filtered_preselect_cte_query_map[self.endpoint_table_info], self.endpoint_table_info, other_local_table_info)
            self.select_map['other_local_table_counts'].append(distinct_count.label(f'{other_local_table_info.name}_count'))

    
    def _get_column_summaries(self):
        # Need to break out virtual table columns and handle them seperatly
        self._build_summary_column_map()

        for table_info, column_type_map in self.summary_column_map.items():
            self.log.debug(f"Constructing column summary select statements for {table_info}")
            table_preselect_cte, preselect_connecting_column, filtered_table_info = self._get_table_preselect_cte(table_info, column_type_map)

            categorical_columns = []
            numeric_columns = []
            data_source_columns = []
            for column in table_preselect_cte.columns:
                matching_column_info = self.db_info.get_column_info(column.name, table_info)
                if matching_column_info in column_type_map['summarizable_columns']:
                    if matching_column_info.column_type == 'categorical':
                        categorical_columns.append(column)
                    elif matching_column_info.column_type == 'numeric':
                        numeric_columns.append(column)
                    # self.get_summarized_select(filtered_table_info, table_info, matching_column_info, column, preselect_connecting_column)
                elif matching_column_info in column_type_map['data_source_columns']:
                    data_source_columns.append(column)

            if table_info in self.db_info.local_table_infos or table_info == self.db_info.get_table_info('project'):
                self._add_categorical_select(table_info, categorical_columns)
            else:
                self._add_null_aware_categorical_select(filtered_table_info, table_info, categorical_columns, preselect_connecting_column)
            self._add_numeric_select(table_info, numeric_columns)
            self._add_data_source_select(table_info, data_source_columns)        

    # def get_summarized_select(self, filtered_table_info, table_info, summarizable_column_info, db_column, connecting_column):
    #     if summarizable_column_info.column_type == 'categorical':
    #         if table_info in self.db_info.local_table_infos or table_info == self.db_info.get_table_info('project'):
    #             self.log.debug(f"Constructing basic categorical summary for {summarizable_column_info}")
    #             column_summary = basic_categorical_summary(self.db, db_column)
    #         else:
    #             self.log.debug(f"Constructing null-aware categorical summary for {summarizable_column_info}")
    #             column_summary = null_aware_categorical_summary(self.db, db_column, connecting_column, summarizable_column_info, self.filtered_preselect_cte_query_map[filtered_table_info])

    #     elif summarizable_column_info.column_type == 'numeric':
    #         self.log.debug(f"Constructing numeric summary for {summarizable_column_info}")
    #         column_summary = numeric_summary(self.db, db_column)
    #     else:
    #         self.log.debug(f'Skipping summarizing {summarizable_column_info} because it is of type: {summarizable_column_info.column_type}')
    #         return
    #     column_label = f'{db_column.name}_summary'
    #     if summarizable_column_info.controlled_term:
    #         self.controlled_term_column_map[db_column.name] = {'data_type': 'single', 'path': [column_label, '*', db_column.name]}
    #     self.select_map[table_info].append(column_summary.label(column_label))

    def _build_summary_column_map(self):
        self.summary_column_map = {}
        for table_info, column_filter_infos in self.table_column_and_filter_map.items():
            table_column_map = {}
            for column_info in column_filter_infos['column_infos']:
                table_info = column_info.parent_table_info
                if table_info not in table_column_map.keys():
                    table_column_map[table_info] = []
                table_column_map[table_info].append(column_info)

            for table_info, column_infos in table_column_map.items():
                self.select_map[table_info] = []
                self.summary_column_map[table_info] = {'data_source_columns': [], 'summarizable_columns': []}
                for column_info in column_infos: 
                    if column_info.summary_returns:
                        if column_info.process_before_display == 'data_source':
                            self.summary_column_map[table_info]['data_source_columns'].append(column_info)
                        else:
                            self.summary_column_map[table_info]['summarizable_columns'].append(column_info)

    def _get_table_preselect_prereqs(self, table_info, column_type_map):
        all_table_columns = []
        table_preselect_joins = []
        for _, column_infos in column_type_map.items():
            for column_info in column_infos:
                db_column, join = get_selectable_db_column_and_possible_join(column_info)
                all_table_columns.append(db_column)
                if join:
                    table_preselect_joins.append(join)
        all_table_columns = list(set(all_table_columns))
        if table_info not in self.filtered_preselect_cte_query_map.keys():
            if table_info.name == 'upstream_identifiers':
                filtered_table_info = self.endpoint_table_info
            else:
                filtered_table_info = table_info.primary_table_info
            connecting_column_info = filtered_table_info.get_table_relationship(table_info).foreign_column_info
        else:
            filtered_table_info = table_info
            connecting_column_info = table_info.primary_key_column_info
            
        all_table_columns = [connecting_column_info.labeled_db_column] + all_table_columns
        return all_table_columns, table_preselect_joins, connecting_column_info, filtered_table_info

    def _get_table_preselect_cte(self, table_info, column_type_map):
        all_table_columns, table_preselect_joins, connecting_column_info, filtered_table_info = self._get_table_preselect_prereqs(table_info, column_type_map)
        table_preselect = self.db.query(*all_table_columns).filter(connecting_column_info.labeled_db_column.in_(self.filtered_preselect_cte_query_map[filtered_table_info]))
        for join in table_preselect_joins:
            table_preselect = table_preselect.join(**join)
            
        # Apply additional filters if required
        if table_info not in self.filtered_preselect_cte_query_map.keys():
            endpoint_relationship = self.endpoint_table_info.get_table_relationship(table_info)
            for additional_filter in endpoint_relationship.additional_filters:
                table_preselect = table_preselect.filter(additional_filter)

        table_preselect_cte = table_preselect.cte(f'{table_info.name}_preselect')
        preselect_connecting_column = get_cte_column(table_preselect_cte, connecting_column_info.name)

        return table_preselect_cte, preselect_connecting_column, filtered_table_info


    def _add_categorical_select(self, table_info, columns):
        category_cte = basic_categorical_summaries(self.db, columns, f'{table_info.name}_categories')
        for column in category_cte.columns:
            column_label = column.name
            self.select_map[table_info].append(self.db.query(column).label(column_label))

    def _add_null_aware_categorical_select(self, filtered_table_info, table_info, columns, connecting_column):
        category_cte = null_aware_categorical_summaries(self.db, columns, table_info, f'{table_info.name}_categories', connecting_column, self.filtered_preselect_cte_query_map[filtered_table_info])
        for column in category_cte.columns:
            column_label = column.name
            self.select_map[table_info].append(self.db.query(column).label(column_label))

    def _add_numeric_select(self, table_info, columns):
        summary_cte = numeric_summaries(self.db, columns, f'{table_info.name}_aggregations')
        for column in summary_cte.columns:
            column_label = column.name.replace('_stats', '_summary')
            self.select_map[table_info].append(self.db.query(column).label(column_label))

    def _add_data_source_select(self, table_info, data_source_columns):
        self.log.debug(f"Cosntructing select statement for the combinations of data_sources for {table_info}")
        if table_info == self.endpoint_table_info:
            label = 'data_source'
        else:
            label = f'{table_info.name}_data_source'
        # self.select_map[table_info].append(data_source_counts(self.db, data_source_columns).label(label))
        self.select_map[table_info].append(data_source_counts_cte(self.db, data_source_columns, f'{table_info.name}_data_source_preselect').label(label))


    def get_query(self):
        subquery = self.db.query(*self.select_clause_columns).subquery('json_subquery')
        query = self.db.query(func.row_to_json(subquery.table_valued()).label('json_results'))
        return query
        

    def get_filter_infos(self, filter_type = None):
        if filter_type:
            return [filter_info for filter_info in self.filter_infos if filter_info.filter_type == filter_type]
        else:
            return self.filter_infos
    