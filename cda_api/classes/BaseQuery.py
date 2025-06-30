from .DatabaseInfo import DatabaseInfo
from .FilterInfo import FilterInfo
from cda_api.db.query_utilities import apply_match_all_and_some_filters, get_cte_column, build_foreign_preselect
from sqlalchemy import func, Label


class BaseQuery:
    def __init__(self, db, db_info: DatabaseInfo, endpoint_table_name, request_body, log):
        self.db = db
        self.db_info = db_info
        self.endpoint_table_info = self.db_info.get_table_info(endpoint_table_name)
        self.request_body = request_body
        self.log = log
        self.filter_infos = []
        
    def _build_filter_infos(self):
        for filter_string in self.request_body.MATCH_ALL:
            self.filter_infos.append(FilterInfo(filter_string, 'match_all', self.db_info, self.log))
        for filter_string in self.request_body.MATCH_SOME:
            self.filter_infos.append(FilterInfo(filter_string, 'match_some', self.db_info, self.log))
    

    def _build_table_column_and_filter_map(self, query_type):
        if query_type == 'summary':
            endpoint_column_infos = self.endpoint_table_info.get_summary_column_infos()
        else:
            endpoint_column_infos = self.endpoint_table_info.get_data_column_infos()
        self.table_column_and_filter_map = {
            self.endpoint_table_info: {
                'column_infos': endpoint_column_infos,
                'filter_infos': []
            }
        }
        if query_type == 'summary':
            self.all_column_infos = self.endpoint_table_info.get_summary_column_infos()
        else:
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
            table_info = None
            column_infos_to_add = []
            if column_to_add.endswith('.*'):
                table_name = column_to_add.replace('.*', '')
                table_info = self.db_info.get_table_info(table_name)
                if query_type == 'summary':
                    column_infos_to_add = table_info.get_summary_column_infos()
                else:
                    column_infos_to_add = table_info.get_data_column_infos()
            elif query_type == 'data':
                if column_to_add == f'{self.endpoint_table_info.name}_identifiers':
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
                if query_type == 'summary':
                    column_infos_to_exclude = table_info.get_summary_column_infos()
                else:
                    column_infos_to_exclude = table_info.get_data_column_infos()
            else:
                column_info = self.db_info.get_column_info(column_to_exclude)
                table_info = column_info.selectable_table_info
                column_infos_to_exclude = [column_info]

            if query_type == 'data':
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

    def get_filter_infos(self, filter_type = None):
        if filter_type:
            return [filter_info for filter_info in self.filter_infos if filter_info.filter_type == filter_type]
        else:
            return self.filter_infos