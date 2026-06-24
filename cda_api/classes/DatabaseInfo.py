from .ColumnInfo import ColumnInfo
from .TableInfo import TableInfo
from .TableRelationship import TableRelationship
from cda_api import get_logger, TableNotFound, ColumnNotFound, RelationshipNotFound
from cda_api.db.connection import session
from sqlalchemy import func, distinct
from sqlalchemy.sql.schema import Column, Table

log = get_logger('DatabaseInfo.py')
setup_log = get_logger("Setup: DatabaseMap.py")

class DatabaseInfo:
    def __init__(self, db_base):
        self.db_base = db_base
        self._build_sqlalchemy_components()
        self._build_column_metadata_map()
        self._build_table_infos()
        self._build_table_relationships()
        self._build_term_table_map()
        self._assign_virtual_table_columns()
        self._assign_null_tables()
        self._assign_null_columns()
        self._assign_foreign_key_column_infos()
        self._assign_primary_table_infos()
        
    def _build_sqlalchemy_components(self):
        setup_log.info("Building variables from automapped Base")
        self.db_tables = self.db_base.metadata.tables
        self.table_names = [table.name for table in self.db_tables.values()]
        self.db_columns = [column for table in self.db_tables.values() for column in table.columns]
        self.column_names = [column.name for column in self.db_columns]

    def _build_column_metadata_map(self):
        # Fetch column_metadata and build a map of table.column to their respective metadata
        setup_log.info("Fetching info from the column_metadata table")
        column_metadata = self.db_tables["column_metadata"]
        db = session()
        subquery = db.query(column_metadata).subquery("json_result")
        query = db.query(func.row_to_json(subquery.table_valued()))
        result = query.all()
        result = [row for (row,) in result]
        self.column_metadata_map = {}
        for row in result:
            table_name = row["cda_table"]
            column_name = row["cda_column"]
            metadata = {k: v for k, v in row.items() if k not in ["cda_table", "cda_column"]}
            if table_name not in self.column_metadata_map.keys():
                self.column_metadata_map[table_name] = {}
            if column_name not in self.column_metadata_map[table_name].keys():
                self.column_metadata_map[table_name][column_name] = metadata
    
    def _build_table_infos(self):
        self.table_infos = []
        self.local_table_infos = []
        self.data_table_infos = []
        self.mapping_table_infos = []
        self.term_table_infos = []
        self.all_column_infos = []
        all_duplicate_column_names = list(set([column_name for column_name in self.column_names if self.column_names.count(column_name) > 1]))
        for db_table in self.db_tables.values():
            table_duplicate_column_names = [column.name for column in db_table.columns if column.name in all_duplicate_column_names]
            table_column_metadata = {}
            if db_table.name in self.column_metadata_map.keys():
                table_column_metadata = self.column_metadata_map[db_table.name]
            table_info = TableInfo(self, db_table, table_column_metadata, table_duplicate_column_names)
            self.table_infos.append(table_info)
            if table_info.name in ['file', 'subject']:
                self.local_table_infos.append(table_info)
            if table_info.name not in ['release_metadata', 'column_metadata']:
                if table_info.name.endswith('_inputs'):
                    pass
                elif table_info.name.endswith('_term'):
                    self.term_table_infos.append(table_info)
                elif ('_describes_' in table_info.name) or ('_in_' in table_info.name) or ('_external_reference' in table_info.name):
                    self.mapping_table_infos.append(table_info)
                else:
                    self.data_table_infos.append(table_info)
            self.all_column_infos.extend(table_info.column_infos)
                    
    
    def _build_table_relationships(self):
        for local_table_info in self.local_table_infos:
            for data_table_info in self.data_table_infos:
                if local_table_info == data_table_info:
                    continue
                # TODO: Double check that we won't ever need to map from subject -> file_keywords or file_text_search and visa-versa
                if data_table_info.name.endswith('_keywords') or data_table_info.name.endswith('_text_search'):
                    if not data_table_info.name.startswith(local_table_info.name):
                        continue
                local_table_info.build_table_relationship(data_table_info)
    
    def _build_term_table_map(self):
        db = session()
        db_columns = []
        joins = []
        full_join = False
        outer_join = True
        connected_term_table_infos = [table_info for table_info in self.term_table_infos if table_info.name != 'controlled_term']
        controlled_term_table_info = self.get_table_info('controlled_term')
        for connected_term_table_info in connected_term_table_infos:
            if connected_term_table_info.name in ['related_term', 'synonym_term']:
                base_term_column = 0
                connected_term_column = 1
            else:
                base_term_column = 1
                connected_term_column = 0
            
            aliased_connected_term_db_table = connected_term_table_info.db_table.alias(f'ct_{connected_term_table_info.name}')
            aliased_controlled_term_db_table = controlled_term_table_info.db_table.alias(f'{connected_term_table_info.name}_ct')

            connected_term_on_clause = controlled_term_table_info.get_column_info('id_alias').db_column == aliased_connected_term_db_table.columns[base_term_column]
            connected_term_join = {'target': aliased_connected_term_db_table, 'onclause': connected_term_on_clause, 'full': full_join, 'isouter': outer_join}

            controlled_term_on_clause = aliased_connected_term_db_table.columns[connected_term_column] == aliased_controlled_term_db_table.columns['id_alias']
            controlled_term_join = {'target': aliased_controlled_term_db_table, 'onclause': controlled_term_on_clause, 'full': full_join, 'isouter': outer_join}
            db_columns.append(func.array_remove(func.array_agg(distinct(aliased_controlled_term_db_table.columns['name'])), None).label(f'{connected_term_table_info.name}s'))
            joins.extend([connected_term_join, controlled_term_join])
            
        q = db.query(controlled_term_table_info.get_column_info('id_alias').db_column, controlled_term_table_info.get_column_info('name').db_column)
        q = q.add_columns(*db_columns)
        for join in joins:
            q = q.join(**join)
        q = q.group_by(controlled_term_table_info.get_column_info('id_alias').db_column)
        subquery = q.subquery('subquery')
        q = db.query(func.row_to_json(subquery.table_valued()).label('json_results'))
        res = q.all()
        self.controlled_term_map = {row[0]['id_alias']: {k:v for k,v in row[0].items() if k != 'id_alias'} for row in res}
        # Add a None result
        self.controlled_term_map[-1] = {k:[] for k,v in res[0][0].items() if k != 'id_alias'}
        self.controlled_term_map[-1]['name'] = None


    
    def _assign_virtual_table_columns(self):
        for column_info in self.all_column_infos:
            if column_info.virtual_table is not None:
                table_info = self.get_table_info(column_info.virtual_table)
                table_info.add_virtual_table_columns(column_info)

    def _assign_null_tables(self):
        for table_info in self.table_infos:
            if table_info.name.endswith('nulls') and table_info.name.replace('_nulls', '') in self.table_names:
                table_info_info_to_assign = self.get_table_info(table_info.name.replace('_nulls', ''))
                table_info_info_to_assign.null_table_info = table_info

    def _assign_null_columns(self):
        for column_info in self.all_column_infos:
            if column_info.name.endswith('null') and column_info.parent_table_info.name.endswith('nulls'):
                column_info_to_assign = self.get_column_info(column_info.name.replace('_null', ''), column_info.parent_table_info.name.replace('_nulls', ''))
                column_info_to_assign.assign_null_column(column_info)
    
    def _assign_foreign_key_column_infos(self):
        for column_info in self.all_column_infos:
            column_info.assign_foreign_key_column_infos()

    def _assign_primary_table_infos(self):
        for table_info in self.table_infos:
            table_info.set_primary_table_info()
    
    def get_column_info(self, column, table = None) -> ColumnInfo:
        if table is None:
            potential_column_infos = []
            if isinstance(column, str):
                potential_column_infos = [column_info for column_info in self.all_column_infos if column_info.name == column]
            elif isinstance(column, Column):
                potential_column_infos = [column_info for column_info in self.all_column_infos if column_info.db_column == column]

            if len(potential_column_infos) < 1:
                # TODO raise better exceptions
                raise ColumnNotFound(f"Column Not Found: {column}")
            elif len(potential_column_infos) > 1:
                # TODO raise better exceptions
                raise ColumnNotFound(f"Unexpectedly found more that one column named: {column}")
            return potential_column_infos[0]
        
        else:
            table_info = self.get_table_info(table)
            return table_info.get_column_info(column)
        
    def get_table_info(self, table) -> TableInfo:
        if isinstance(table, str):
            potential_table_infos = [table_info for table_info in self.table_infos if table_info.name == table]
        elif isinstance(table, Table):
            potential_table_infos = [table_info for table_info in self.table_infos if table_info.db_table == table]
        elif isinstance(table, TableInfo):
            return table
        else:
            raise Exception(f"Unexpected type {type(table)} for foreign_table. Only expecting str, Table, or TableInfo")
        if len(potential_table_infos) < 1:
            # TODO raise better exceptions
            raise TableNotFound(f"Table not found: {table}")
        elif len(potential_table_infos) > 1:
            # TODO raise better exceptions
            raise TableNotFound(f"Unexpectedly found more that one table named: {table}")
        return potential_table_infos[0]
    
    def get_table_relationship(self, local_table, foreign_table) -> TableRelationship:
        local_table_info = self.get_table_info(local_table)
        if local_table_info not in self.local_table_infos:
            raise RelationshipNotFound(f'Unexpected local table: {local_table}. Should only be from following list of tables {[table_info.name for table_info in self.local_table_infos]}')

        return local_table_info.get_table_relationship(foreign_table)
    
    def reset(self, db_base):
        self.__init__(db_base)
