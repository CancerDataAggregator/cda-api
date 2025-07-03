from cda_api.db.query_functions import get_cte_column, column_distinct_count_subquery, foreign_table_distinct_count, data_source_counts, basic_categorical_summary, null_aware_categorical_summary, numeric_summary
from .models import SummaryRequestBody
from .DatabaseInfo import DatabaseInfo
from .shared_class_functions import get_filter_infos, get_table_column_and_filter_map, get_filtered_preselect
from sqlalchemy import func

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
        self.filter_infos = get_filter_infos(self)
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
        self.summary_column_map = {}
        # Need to break out virtual table columns and handle them seperatly
        for table_info, column_filter_infos in self.table_column_and_filter_map.items():
            table_column_map = {}
            for column_info in column_filter_infos['column_infos']:
                table_info = column_info.parent_table_info
                if table_info not in table_column_map.keys():
                    table_column_map[table_info] = []
                table_column_map[table_info].append(column_info)

            for table_info, column_infos in table_column_map.items():
                self.add_table_to_summary_column_map(table_info, column_infos)
                self.select_map[table_info] = []

        for table_info, column_type_map in self.summary_column_map.items():
            self.log.debug(f"Constructing column summary selecct statements for {table_info}")
            all_table_columns = list(set([column_info.labeled_db_column for _, column_infos in column_type_map.items() for column_info in column_infos]))
            if table_info not in self.filtered_preselect_cte_query_map.keys():
                connecting_column_info = table_info.primary_table_info.get_table_relationship(table_info).foreign_column_info
            else:
                connecting_column_info = table_info.primary_key_column_info
                
            all_table_columns = [connecting_column_info.labeled_db_column] + all_table_columns

            table_preselect = self.db.query(*all_table_columns).filter(connecting_column_info.labeled_db_column.in_(self.filtered_preselect_cte_query_map[table_info.primary_table_info]))
            table_preselect_cte = table_preselect.cte(f'{table_info.name}_preselect')
            preselect_connecting_column = get_cte_column(table_preselect_cte, connecting_column_info.name)

            data_source_columns = []
            for column in table_preselect_cte.columns:
                matching_column_info = self.db_info.get_column_info(column.name, table_info)
                if matching_column_info in column_type_map['summarizable_columns']:
                    self.get_summarized_select(table_info, matching_column_info, column, preselect_connecting_column)
                elif matching_column_info in column_type_map['data_source_columns']:
                    data_source_columns.append(column)
            
            if data_source_columns:
                self.log.debug(f"Cosntructing select statement for the combinations of data_sources for {table_info}")
                if table_info == self.endpoint_table_info:
                    label = 'data_source'
                else:
                    label = f'{table_info.name}_data_source'
                self.select_map[table_info].append(data_source_counts(self.db, data_source_columns).label(label))


    def add_table_to_summary_column_map(self, table_info, column_infos):
        self.summary_column_map[table_info] = {'data_source_columns': [], 'summarizable_columns': []}
        for column_info in column_infos:  
            if column_info.process_before_display == 'data_source':
                self.summary_column_map[table_info]['data_source_columns'].append(column_info)
            else:
                self.summary_column_map[table_info]['summarizable_columns'].append(column_info)


    def get_summarized_select(self, table_info, summarizable_column_info, db_column, connecting_column):
        if summarizable_column_info.column_type == 'categorical':
            if table_info in self.db_info.local_table_infos:
                self.log.debug(f"Constructing basic categorical summary for {summarizable_column_info}")
                column_summary = basic_categorical_summary(self.db, db_column)
            else:
                self.log.debug(f"Constructing null-aware categorical summary for {summarizable_column_info}")
                column_summary = null_aware_categorical_summary(self.db, db_column, connecting_column)

        elif summarizable_column_info.column_type == 'numeric':
            self.log.debug(f"Constructing numeric summary for {summarizable_column_info}")
            column_summary = numeric_summary(self.db, db_column)
        else:
            self.log.debug(f'Skipping summarizing {summarizable_column_info} because it is of type: {summarizable_column_info.column_type}')
            return

        self.select_map[table_info].append(column_summary.label(f'{db_column.name}_summary'))


    def get_query(self):
        subquery = self.db.query(*self.select_clause_columns).subquery('json_result')
        query = self.db.query(func.row_to_json(subquery.table_valued()).label('results'))
        return query
        

    def get_filter_infos(self, filter_type = None):
        if filter_type:
            return [filter_info for filter_info in self.filter_infos if filter_info.filter_type == filter_type]
        else:
            return self.filter_infos
    