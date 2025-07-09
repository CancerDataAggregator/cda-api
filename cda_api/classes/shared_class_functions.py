from .FilterInfo import FilterInfo
from cda_api.db.query_functions import get_cte_column, apply_match_all_and_some_filters

def get_filter_infos(query_object):
    log = query_object.log
    log.debug("Constructing FilterInfo objects from MATCH_ALL and MATCH_SOME arguments")
    filter_infos = [FilterInfo(filter_string, 'match_all', query_object.db_info, query_object.log) for filter_string in query_object.request_body.MATCH_ALL]
    filter_infos.extend([FilterInfo(filter_string, 'match_some', query_object.db_info, query_object.log) for filter_string in query_object.request_body.MATCH_SOME])
    return filter_infos
    

def get_table_column_and_filter_map(query_object, query_type):
    log = query_object.log
    log.debug(f"Constructing map between tables to their columns and filters for a {query_type} query")
    table_column_and_filter_map = {
        query_object.endpoint_table_info: {
            'column_infos': query_object.endpoint_table_info.get_column_infos(query_type),
            'filter_infos': []
        }
    }
    all_column_infos = query_object.endpoint_table_info.get_column_infos(query_type)

    # Adding filter columns
    for filter_info in query_object.get_filter_infos():
        filter_column_info = filter_info.selectable_column_info
        filter_table_info = filter_column_info.selectable_table_info
        if filter_table_info not in table_column_and_filter_map.keys():
            table_column_and_filter_map[filter_table_info] = {'column_infos': [], 'filter_infos': []}
        if filter_column_info not in all_column_infos:
            query_object.log.debug(f'Adding filter column {filter_column_info} to table_map')
            table_column_and_filter_map[filter_table_info]['column_infos'].append(filter_column_info)
            all_column_infos.append(filter_column_info)
        table_column_and_filter_map[filter_table_info]['filter_infos'].append(filter_info)
        
    # Adding from ADD_COLUMNS
    for column_to_add in query_object.request_body.ADD_COLUMNS:
        query_object.log.debug(f'Adding {column_to_add}')
        if column_to_add.endswith('.*'):
            table_name = column_to_add.replace('.*', '')
            table_info = query_object.db_info.get_table_info(table_name)
            column_infos_to_add = table_info.get_column_infos(query_type)
        else:
            column_info = query_object.db_info.get_column_info(column_to_add)
            table_info = column_info.selectable_table_info
            column_infos_to_add = [column_info]

        if table_info not in table_column_and_filter_map.keys():
            table_column_and_filter_map[table_info] = {'column_infos': [], 'filter_infos': []}
        column_infos_to_add = [column_info for column_info in column_infos_to_add if column_info not in all_column_infos]
        table_column_and_filter_map[table_info]['column_infos'].extend(column_infos_to_add)
        all_column_infos.extend(column_infos_to_add)

    # Excluding from EXCLUDE_COLUMNS
    for column_to_exclude in query_object.request_body.EXCLUDE_COLUMNS:
        if column_to_exclude.endswith('.*'):
            table_name = column_to_exclude.replace('.*', '')
            table_info = query_object.db_info.get_table_info(table_name)
            column_infos_to_exclude = table_info.get_data_column_infos()
        else:
            column_info = query_object.db_info.get_column_info(column_to_exclude)
            table_info = column_info.selectable_table_info
            column_infos_to_exclude = [column_info]

        
        if table_info in table_column_and_filter_map.keys():
            new_columns = [column_info for column_info in table_column_and_filter_map[table_info]['column_infos'] if column_info not in column_infos_to_exclude]
            table_column_and_filter_map[table_info]['column_infos'] = new_columns

    if query_type == 'data':
        if query_object.request_body.EXTERNAL_REFERENCE:
            external_reference_table_info = query_object.db_info.get_table_info('external_reference')
            column_infos = [column_info for column_info in external_reference_table_info.column_infos 
                            if column_info.process_before_display == 'external_reference_metadata']
            table_column_and_filter_map[external_reference_table_info] = {'column_infos': column_infos, 'filter_infos': []}

    return table_column_and_filter_map


def get_filtered_preselect(query_object):
    log = query_object.log
    log.debug("Constructing filtered preselect")
    mapping_table_infos = []
    for table_info in query_object.table_column_and_filter_map.keys():
        if table_info != query_object.endpoint_table_info:
            table_relationship = query_object.db_info.get_table_relationship(query_object.endpoint_table_info, table_info)
            if table_relationship.requires_mapping_table:
                if not table_relationship.local_mapping_column_info.parent_table_info.name.endswith('external_reference'):
                    mapping_table_infos.append(table_relationship.local_mapping_column_info.parent_table_info)
    mapping_table_infos = list(set(mapping_table_infos))
    filter_preselect_map = {}
    filtered_preselect_joins = []
    if len(mapping_table_infos) < 1:
        log.debug(f'Only need to construct filtered preselect from {query_object.endpoint_table_info}')
        filter_preselect_map[query_object.endpoint_table_info] = query_object.endpoint_alias
    else:
        for mapping_table_info in mapping_table_infos:
            log.debug(f'Including {mapping_table_info} in the filtered preselect')
            mapping_table_columns = mapping_table_info.column_infos
            for column_info in mapping_table_columns:
                mapping_fk_column_info = column_info.foreign_key_column_info
                if mapping_fk_column_info is None:
                    raise Exception('Only expected mapping columns which have foreign keys')
                if mapping_fk_column_info.parent_table_info not in filter_preselect_map.keys():
                    filter_preselect_map[mapping_fk_column_info.parent_table_info] = column_info
                else:
                    column_info_to_join = filter_preselect_map[mapping_fk_column_info.parent_table_info]
                    filtered_preselect_joins.append({'target': mapping_table_info.db_table, 'onclause': column_info.db_column == column_info_to_join.db_column})

    preselect_columns = [column_info.labeled_db_column for column_info in filter_preselect_map.values()]
    filtered_preselect = query_object.db.query(*preselect_columns)
    for mapping_join in filtered_preselect_joins:
        filtered_preselect = filtered_preselect.join(**mapping_join)

    match_all_db_filters  = [filter_info.get_filterable_preselect(filter_preselect_map, query_object.endpoint_table_info) for filter_info in query_object.get_filter_infos('match_all')]
    match_some_db_filters = [filter_info.get_filterable_preselect(filter_preselect_map, query_object.endpoint_table_info) for filter_info in query_object.get_filter_infos('match_some')]

    log.debug(f'Applying MATCH_ALL and MATCH_SOME filters to the filtered preselect')
    preselect_cte = apply_match_all_and_some_filters(filtered_preselect, match_all_db_filters, match_some_db_filters)
    preselect_cte_name = f'filtered_preselect'
    preselect_cte = preselect_cte.cte(preselect_cte_name)
    filtered_preselect = query_object.db.query(preselect_cte.c)
    filtered_preselect_cte_query_map = {}
    filtered_preselect_column_map = {}
    for table_info, column_info in filter_preselect_map.items():
        cte_column = get_cte_column(preselect_cte, column_info.name)
        filtered_preselect_cte_query_map[table_info] = query_object.db.query(cte_column)
        filtered_preselect_column_map[table_info] = cte_column
    log.debug('Filtered preselect construction complete')
    return filtered_preselect, filtered_preselect_cte_query_map, filtered_preselect_column_map