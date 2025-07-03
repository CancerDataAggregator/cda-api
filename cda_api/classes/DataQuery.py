from cda_api.db.query_functions import build_foreign_preselect
from .models import DataRequestBody
from .DatabaseInfo import DatabaseInfo
from .shared_class_functions import get_filter_infos, get_table_column_and_filter_map, get_filtered_preselect
from sqlalchemy import func, Label

class DataQuery:
    def __init__(self, db, db_info: DatabaseInfo, endpoint_table_name, request_body: DataRequestBody, log):
        # Initailize arguments
        self.db = db
        self.db_info = db_info
        self.endpoint_table_info = self.db_info.get_table_info(endpoint_table_name)
        self.request_body = request_body
        self.log = log

        # Set useful variables
        self.endpoint_alias = self.endpoint_table_info.primary_key_column_info

        # Construct filter preselect
        self.filter_infos = get_filter_infos(self)
        self.table_column_and_filter_map = get_table_column_and_filter_map(self, 'data')
        self.filtered_preselect, self.filtered_preselect_cte_query_map, self.filtered_preselect_column_map = get_filtered_preselect(self)

        # Build select columns and joins
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
            
            select_columns = []
            select_joins = []

            # Add endpoint table select columns:
            if table_info == self.endpoint_table_info:
                virtual_column_map = {}
                table_virtual_column_infos = table_info.virtual_column_infos
                local_select_columns = [column_info.labeled_db_column for column_info in column_infos if column_info not in table_virtual_column_infos]
                self.select_map[table_info][table_info.name] = local_select_columns

                # Need to build mapping of virtual_tables to their respective columns
                for column_info in column_infos:
                    if column_info not in table_virtual_column_infos:
                        continue
                    virtual_table_info = column_info.parent_table_info
                    if virtual_table_info not in virtual_column_map.keys():
                        virtual_column_map[virtual_table_info] = []
                    virtual_column_map[virtual_table_info].append(column_info)

                # Using the virtual table mapping we need to add the distince array columns to the select statement and include their joins
                if virtual_column_map:
                    for virtual_table_info, v_column_infos in virtual_column_map.items():
                        construct_type = 'array'
                        related_filtered_preselect_query = self.filtered_preselect_cte_query_map[self.endpoint_table_info]
                        virtual_select_columns, virtual_select_joins = build_foreign_preselect(construct_type, self.db, self.endpoint_table_info, self.endpoint_table_info, related_filtered_preselect_query, virtual_table_info, v_column_infos, filter_infos, self.log)
                        select_columns.extend(virtual_select_columns)
                        select_joins.extend(virtual_select_joins)
            
            # Add foreign table select columns:
            else:
                if self.request_body.COLLATE_RESULTS is False:
                    construct_type = 'array'
                else:
                    construct_type = 'json'

                if table_info.name == 'upstream_identifiers':
                    relating_table_info = self.endpoint_table_info
                else:
                    relating_table_info = table_info.primary_table_info
                related_filtered_preselect_query = self.filtered_preselect_cte_query_map[relating_table_info]

                foreign_select_columns, foreign_select_joins = build_foreign_preselect(construct_type, self.db, self.endpoint_table_info, relating_table_info, related_filtered_preselect_query, table_info, column_infos, filter_infos, self.log)
                select_columns.extend(foreign_select_columns)
                select_joins.extend(foreign_select_joins)
            
            # Add the select columns where they belong in the select_map
            for select_column in select_columns:
                if isinstance(select_column, Label):
                    preselect_name = select_column.element.table.name
                else:
                    preselect_name = select_column.table.name
                if preselect_name not in self.select_map[table_info].keys():
                    self.select_map[table_info][preselect_name] = []
                if construct_type == 'array': # Need to coalesce to an empty list in place of Null
                    select_column = func.coalesce(select_column, []).label(select_column.name)
                self.select_map[table_info][preselect_name].append(select_column)

            self.select_joins.extend(select_joins)

        endpoint_columns = []
        provenance_columns = []
        filter_columns = []
        add_columns = []
        for table_info, select_table_map in self.select_map.items():
            for select_table, select_columns in select_table_map.items():
                for select_column in select_columns:
                    if table_info == self.endpoint_table_info:
                        endpoint_columns.append(select_column)
                    elif select_column.name.endswith('identifiers'):
                        provenance_columns.append(select_column)
                    elif select_column.name in [filter_info.selectable_column_info.name for filter_info in self.table_column_and_filter_map[table_info]['filter_infos']]:
                        filter_columns.append(select_column)
                    else:
                        add_columns.append(select_column)

        self.select_columns = endpoint_columns + provenance_columns + filter_columns + add_columns  


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