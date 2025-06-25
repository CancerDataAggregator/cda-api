from cda_api.models import DataRequestBody
from cda_api.db.query_utilities import apply_match_all_and_some_filters, get_cte_column, build_foreign_preselect
from .DatabaseInfo import DatabaseInfo
from .FilterInfo import FilterInfo
from sqlalchemy import func, Label

class DataQuery:
    def __init__(self, db, db_info: DatabaseInfo, endpoint_table_name, request_body: DataRequestBody, log):
        self.db = db
        self.db_info = db_info
        self.endpoint_table_info = self.db_info.get_table_info(endpoint_table_name)
        self.request_body = request_body
        self.log = log
        self.filter_infos = []

        self._build_filter_infos()
        self._build_table_column_and_filter_map()
        self._build_filtered_preselect()
        self._build_select_columns_and_joins()

    def __repr__(self):
        table_column_filter_map_string = "\n".join([f"\t{table}\n\t\tcolumn_infos: {m['column_infos']}\n\t\tfilter_infos: {m['filter_infos']}" for table, m in self.table_column_and_filter_map.items()])
        select_map_string = "\n".join([f"\t{table_info}\n{'\n'.join(f'\t\t{select_table}\n\t\t\t{[select_column.name for select_column in select_columns]}' for select_table, select_columns in table_select_map.items() )}" for table_info, table_select_map in self.select_map.items() ])
        select_joins_string = "\n".join([f"\t{j['target'].name} on: \n\t\t{str(j['onclause'])}" for j in self.select_joins])
        select_columns_string_list = []
        for select_column in self.select_columns:
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
            f'DataQuery({self.log.extra['id']})',
            f'Endpoint: {self.endpoint_table_info}', 
            f'MATCH_ALL Filters:\n{self.get_filter_infos('match_all')}',
            f'MATCH_SOME Filters:\n{self.get_filter_infos('match_some')}',
            f'Table Column and Filter Map:',
            f'{table_column_filter_map_string}',
            f'Select Map:',
            f'{select_map_string}',
            f'Select Joins:',
            f'{select_joins_string}',
            f'Ordered Select Columns',
            f'{select_columns_string}'
        ]
        return '\n'.join(repr_components)


    def _build_filter_infos(self):
        for filter_string in self.request_body.MATCH_ALL:
            self.filter_infos.append(FilterInfo(filter_string, 'match_all', self.db_info, self.log))
        for filter_string in self.request_body.MATCH_SOME:
            self.filter_infos.append(FilterInfo(filter_string, 'match_some', self.db_info, self.log))
    

    def _build_table_column_and_filter_map(self):
        self.table_column_and_filter_map = {
            self.endpoint_table_info: {
                'column_infos': self.endpoint_table_info.get_data_column_infos(),
                'filter_infos': []
            }
        }
        self.all_column_infos = self.endpoint_table_info.get_data_column_infos()
        self.add_identifiers = False

        # Adding filter columns
        for filter_info in self.get_filter_infos():
            filter_column_info = filter_info.selectable_column_info
            filter_table_info = filter_column_info.selectable_table_info
            if filter_table_info not in self.table_column_and_filter_map.keys():
                self.table_column_and_filter_map[filter_table_info] = {'column_infos': [], 'filter_infos': []}
            if filter_column_info not in self.all_column_infos:
                self.table_column_and_filter_map[filter_table_info]['column_infos'].append(filter_column_info)
                self.all_column_infos.append(filter_column_info)
            self.table_column_and_filter_map[filter_table_info]['filter_infos'].append(filter_info)

        # Adding from ADD_COLUMNS
        for column_to_add in self.request_body.ADD_COLUMNS:
            self.log.debug(f'Adding {column_to_add}')
            if column_to_add.endswith('.*'):
                table_name = column_to_add.replace('.*', '')
                table_info = self.db_info.get_table_info(table_name)
                column_infos_to_add = table_info.get_data_column_infos()
            elif column_to_add == f'{self.endpoint_table_info.name}_identifiers':
                self.add_identifiers = True
                table_name = 'upstream_identifiers'
                table_info = self.db_info.get_table_info(table_name)
                column_infos_to_add = table_info.get_summary_process_before_display_column_infos()
            else:
                column_info = self.db_info.get_column_info(column_to_add)
                table_info = column_info.selectable_table_info
                column_infos_to_add = [column_info]

            if table_info not in self.table_column_and_filter_map.keys():
                self.table_column_and_filter_map[table_info] = {'column_infos': [], 'filter_infos': []}
            column_infos_to_add = [column_info for column_info in column_infos_to_add if column_info not in self.all_column_infos]
            self.table_column_and_filter_map[table_info]['column_infos'].extend(column_infos_to_add)
            self.all_column_infos.extend(column_infos_to_add)

        # Excluding from EXCLUDE_COLUMNS
        for column_to_exclude in self.request_body.EXCLUDE_COLUMNS:
            if column_to_exclude.endswith('.*'):
                table_name = column_to_exclude.replace('.*', '')
                table_info = self.db_info.get_table_info(table_name)
                column_infos_to_exclude = table_info.get_data_column_infos()
            else:
                column_info = self.db_info.get_column_info(column_to_exclude)
                table_info = column_info.parent_table_info
                column_infos_to_exclude = [column_info]

            if table_info.name == 'upstream_identifiers' and self.add_identifiers:
                continue
            
            if table_info in self.table_column_and_filter_map.keys():
                new_columns = [column_info for column_info in self.table_column_and_filter_map[table_info]['column_infos'] if column_info not in column_infos_to_exclude]
                self.table_column_and_filter_map[table_info]['column_infos'] = new_columns

    def _build_filtered_preselect(self):
        self.endpoint_alias = self.endpoint_table_info.primary_key_column_info
        mapping_table_infos = []
        for table_info in self.table_column_and_filter_map.keys():
            if table_info != self.endpoint_table_info:
                table_relationship = self.db_info.get_table_relationship(self.endpoint_table_info, table_info)
                if table_relationship.requires_mapping_table:
                    mapping_table_infos.append(table_relationship.local_mapping_column_info.parent_table_info)
        mapping_table_infos = list(set(mapping_table_infos))

        self.filter_preselect_map = {}
        filtered_preselect_joins = []
        if len(mapping_table_infos) < 1:
            self.filter_preselect_map[self.endpoint_table_info] = self.endpoint_table_info.primary_key_column_info
        else:
            for mapping_table_info in mapping_table_infos:
                mapping_table_columns = mapping_table_info.column_infos
                for column_info in mapping_table_columns:
                    mapping_fk_column_info = column_info.foreign_key_column_info
                    if mapping_fk_column_info is None:
                        raise Exception('Only expected mapping columns which have foreign keys')
                    if mapping_fk_column_info.parent_table_info not in self.filter_preselect_map.keys():
                        self.filter_preselect_map[mapping_fk_column_info.parent_table_info] = column_info
                    else:
                        column_info_to_join = self.filter_preselect_map[mapping_fk_column_info.parent_table_info]
                        filtered_preselect_joins.append({'target': mapping_table_info.db_table, 'onclause': column_info.db_column == column_info_to_join.db_column})

        preselect_columns = [column_info.labeled_db_column for column_info in self.filter_preselect_map.values()]
        filtered_preselect = self.db.query(*preselect_columns)
        for mapping_join in filtered_preselect_joins:
            filtered_preselect = filtered_preselect.join(**mapping_join)

        match_all_db_filters  = [filter_info.get_filterable_preselect(self.filter_preselect_map) for filter_info in self.get_filter_infos('match_all')]
        match_some_db_filters = [filter_info.get_filterable_preselect(self.filter_preselect_map) for filter_info in self.get_filter_infos('match_some')]

        preselect_cte = apply_match_all_and_some_filters(filtered_preselect, match_all_db_filters, match_some_db_filters)
        preselect_cte_name = f'filtered_preselect'
        preselect_cte = preselect_cte.cte(preselect_cte_name)
        self.filtered_preselect = self.db.query(preselect_cte.c)
        self.filtered_preselect_cte_query_map = {}
        for table_info, column_info in self.filter_preselect_map.items():
            cte_column = get_cte_column(preselect_cte, column_info.name)
            self.filtered_preselect_cte_query_map[table_info] = self.db.query(cte_column)

    def _build_select_columns_and_joins(self):
        self.select_map = {}
        self.select_joins = []
        for table_info, value in self.table_column_and_filter_map.items():
            column_infos = value['column_infos']
            filter_infos = value['filter_infos']
            self.select_map[table_info] = {}

            if len(column_infos) == 0:
                self.log.debug(f'Skipping {table_info} because there were no columns to select from after applying EXCLUDE_COLUMNS')
                continue
            
            if table_info == self.endpoint_table_info:
                self.select_map[table_info][table_info.name] = [column_info.labeled_db_column for column_info in column_infos]
                continue

            if table_info.name == 'upstream_identifiers' and self.add_identifiers:
                construct_type = 'provenance'
            elif self.request_body.EXPAND_RESULTS is False:
                construct_type = 'array'
            else:
                construct_type = 'json'

            
            if table_info.name == 'upstream_identifiers':
                relating_table_info = self.endpoint_table_info
            else:
                relating_table_info = table_info.primary_table_info
            related_filtered_preselect_query = self.filtered_preselect_cte_query_map[relating_table_info]

            select_columns, select_joins = build_foreign_preselect(construct_type, self.db, self.endpoint_table_info, relating_table_info, related_filtered_preselect_query, table_info, column_infos, filter_infos, self.log)

            for select_column in select_columns:
                if isinstance(select_column, Label):
                    preselect_name = select_column.element.table.name
                else:
                    preselect_name = select_column.table.name
                if preselect_name not in self.select_map[table_info].keys():
                    print(table_info, preselect_name)
                    self.select_map[table_info][preselect_name] = []
                if construct_type == 'array':
                    select_column = func.coalesce(select_column, []).label(select_column.name)
                self.select_map[table_info][preselect_name].append(select_column)

            self.select_joins.extend(select_joins)

        endpoint_columns = []
        provenance_column = []
        filter_columns = []
        add_columns = []
        for table_info, select_table_map in self.select_map.items():
            for select_table, select_columns in select_table_map.items():
                for select_column in select_columns:
                    if table_info == self.endpoint_table_info:
                        endpoint_columns.append(select_column)
                    elif select_column.name.endswith('identifiers'):
                        provenance_column.append(select_column)
                    elif select_column.name in [filter_info.selectable_column_info.name for filter_info in self.table_column_and_filter_map[table_info]['filter_infos']]:
                        filter_columns.append(select_column)
                    else:
                        add_columns.append(select_column)

        self.select_columns = endpoint_columns + provenance_column + filter_columns + add_columns  


    def get_query(self):
        query = self.db.query(*self.select_columns)
        query = query.filter(self.endpoint_alias.db_column.in_(self.filtered_preselect_cte_query_map[self.endpoint_table_info]))
        for join in self.select_joins:
            query = query.join(**join, isouter=True)
        subquery = query.subquery("json_result")
        return self.db.query(func.row_to_json(subquery.table_valued()))
    
    def get_count_query(self):
        count_subquery = (
            self.db.query(self.endpoint_alias.db_column).filter(self.endpoint_alias.db_column.in_(self.filtered_preselect_cte_query_map[self.endpoint_table_info])).subquery("rows_to_count")
        )
        return self.db.query(func.count()).select_from(count_subquery)


    def get_filter_infos(self, filter_type = None):
        if filter_type:
            return [filter_info for filter_info in self.filter_infos if filter_info.filter_type == filter_type]
        else:
            return self.filter_infos