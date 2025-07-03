import itertools

import sqlparse
from sqlalchemy import CTE, Label, and_, distinct, func, or_, SelectLabelStyle, union_all, union
from sqlalchemy.exc import CompileError


from cda_api import RelationshipNotFound, get_logger, TableNotFound


log = get_logger()


# Generates compiled SQL string from query object
def query_to_string(q, indented=False) -> str:
    try:
        sql_string = str(q.statement.compile(compile_kwargs={"literal_binds": True}))
        if indented:
            return sqlparse.format(sql_string, reindent=True, keyword_case="upper")
        else:
            return sql_string.replace("\n", "")
    except CompileError as ce:
        sql_string = str(q.statement.compile())
        if indented:
            return sqlparse.format(sql_string, reindent=True, keyword_case="upper")
        else:
            return sql_string.replace("\n", "")
    except Exception as e:
        raise e


# Prints compiled SQL string from query object
def print_query(q) -> None:
    print(query_to_string(q, indented=True))


# Returns the column object from a cte (Common Table Expression) object
def get_cte_column(cte, columnname):
    return getattr(cte.c, columnname)


# Get array aggregate of unique values in a column not including null
def unique_column_array_agg(column):
    return func.array_remove(func.array_agg(distinct(column)), None).label(column.name)
    
    
def apply_match_all_and_some_filters(query, match_all_db_filters, match_some_db_filters):
    if match_all_db_filters and match_some_db_filters:
        query = query.filter(and_(*match_all_db_filters)).filter(or_(*match_some_db_filters))
    elif match_all_db_filters:
        query = query.filter(and_(*match_all_db_filters))
    elif match_some_db_filters:
        query = query.filter(or_(*match_some_db_filters))
    return query


def build_virtual_foreign_arrays(db,foreign_table_info, virtual_table_info, virtual_column_infos, filtered_preselect, log):
    virtual_table_relationship = foreign_table_info.get_table_relationship(virtual_table_info)
    if virtual_table_relationship.requires_mapping_table:
        relating_column = virtual_table_relationship.local_mapping_column_info.db_column
    else:
        relating_column = virtual_table_relationship.foreign_column_info.db_column
    cte_name = f'{foreign_table_info.name}_{virtual_table_info.name}_columns'
    select_columns = [relating_column] + [unique_column_array_agg(column_info.db_column).label(column_info.name) for column_info in virtual_column_infos]
    virtual_preselect = (
        db.query(*select_columns)
        .filter(relating_column.in_(filtered_preselect))
    )
    virtual_preselect = virtual_preselect.group_by(relating_column)
    virtual_preselect_cte = virtual_preselect.cte(cte_name)
    preselect_onclause = get_cte_column(virtual_preselect_cte, relating_column.name) == virtual_table_relationship.local_column_info.db_column
    preselect_join = {'target': virtual_preselect_cte, 'onclause': preselect_onclause}
    preselect_columns = [db_column for db_column in virtual_preselect_cte.c if db_column.name != relating_column.name]
    return preselect_columns, [preselect_join]


def build_foreign_preselect(construct_type, db, endpoint_table_info, relating_table_info, filtered_preselect, foreign_table_info, column_infos, foreign_filter_infos, log):
    log.debug(f'Building foreign array or {foreign_table_info} connecting to {endpoint_table_info} through {relating_table_info}')
    # Get relationship info
    if relating_table_info != foreign_table_info:
        filter_relationship = relating_table_info.get_table_relationship(foreign_table_info)
        if filter_relationship.requires_mapping_table:
            filtered_preselect_relating_column = filter_relationship.local_mapping_column_info.db_column
        else:
            filtered_preselect_relating_column = filter_relationship.foreign_column_info.db_column
    else:
        filtered_preselect_relating_column = relating_table_info.primary_key_column_info.db_column

    endpoint_relationship = endpoint_table_info.get_table_relationship(foreign_table_info)
    if endpoint_relationship.requires_mapping_table:
        endpoint_relating_column = endpoint_relationship.local_mapping_column_info.db_column
    else:
        endpoint_relating_column = endpoint_relationship.foreign_column_info.db_column

    virtual_column_info_map = {}
    foreign_column_infos = []
    for column_info in column_infos:
        if column_info.parent_table_info != foreign_table_info:
            virtual_table_info = column_info.parent_table_info
            if virtual_table_info not in virtual_column_info_map.keys():
                virtual_column_info_map[virtual_table_info] = []
            virtual_column_info_map[virtual_table_info].append(column_info)
        else:
            foreign_column_infos.append(column_info)

    # Name and select columns slightly differ between json & array
    if construct_type == 'json':
        cte_name = f"{foreign_table_info.name}_collated_preselect"
        select_columns = [endpoint_relating_column] + [column_info.labeled_db_column for column_info in foreign_column_infos]
    elif construct_type == 'array':
        cte_name = f'{foreign_table_info.name}_{endpoint_table_info.name}_columns'
        select_columns = [endpoint_relating_column] + [unique_column_array_agg(column_info.db_column).label(column_info.name) for column_info in foreign_column_infos]
    else:
        raise Exception(f'Unexpected foreign preselect contruct_type {construct_type}. Please use "json", or "array"')
    
    virtual_table_joins = []
    if construct_type == 'array':
        for virtual_table_info, virtual_column_infos in virtual_column_info_map.items():
            select_columns += [unique_column_array_agg(column_info.db_column).label(column_info.name) for column_info in virtual_column_infos]
            virtual_table_relationship = foreign_table_info.get_table_relationship(virtual_table_info)
            virtual_table_joins.append(virtual_table_relationship.get_foreign_table_join_clause())
    else:
        for virtual_table_info, virtual_column_infos in virtual_column_info_map.items():
            columns, joins = build_virtual_foreign_arrays(db, foreign_table_info, virtual_table_info, virtual_column_infos, filtered_preselect, log)
            if construct_type == 'json':
                for col in columns:
                    select_columns.append(func.coalesce(col, []).label(col.name))
            else:
                select_columns.extend(columns)
            virtual_table_joins.extend(joins)

    log.debug(f"Building {cte_name}")


    # Set up base preselect
    foreign_preselect = (
        db.query(*select_columns)
        .filter(filtered_preselect_relating_column.in_(filtered_preselect))
    )

    for virtual_table_join in virtual_table_joins:
        foreign_preselect = foreign_preselect.join(**virtual_table_join, isouter=True)

    # Join on the mapping table if required
    if endpoint_relationship.requires_mapping_table:
        foreign_preselect = foreign_preselect.join(**endpoint_relationship.get_foreign_table_join_clause())
    
    # Apply additional filters if required
    for additional_filter in endpoint_relationship.additional_filters:
        foreign_preselect = foreign_preselect.filter(additional_filter)

    # Build individual column arrays
    if construct_type == 'array': 
        foreign_preselect = foreign_preselect.group_by(endpoint_relating_column)
        foreign_array_preselect_cte = foreign_preselect.cte(cte_name)
        preselect_onclause = get_cte_column(foreign_array_preselect_cte, endpoint_relating_column.name) == endpoint_relationship.local_column_info.db_column
        preselect_join = {'target': foreign_array_preselect_cte, 'onclause': preselect_onclause}
        preselect_columns = [db_column for db_column in foreign_array_preselect_cte.c if db_column.name != endpoint_relating_column.name]
        
    # Build json for all columns
    elif construct_type == 'json': 
        # Need to auto add data_at and data_source columns
        foreign_table_subquery = foreign_preselect.subquery('subquery')
        foreign_json_columns = []
        subquery_id_column = None
        for column in foreign_table_subquery.c:
            if column.name != endpoint_relating_column.name:
                foreign_json_columns.append(column.name)
                foreign_json_columns.append(column)
            else:
                subquery_id_column = column
        
        json_subquery = db.query(
                            subquery_id_column.label(endpoint_relating_column.name),
                            func.json_build_object(*foreign_json_columns).label('json_results')
                        ).subquery('json_subquery')
        
        json_subquery_id_column = json_subquery.c[endpoint_relating_column.name]

        log.debug(f"Aggregating rows")
        foreign_json_preselect = (
            db.query(
                json_subquery_id_column.label(endpoint_relating_column.name),
                func.array_agg(json_subquery.c['json_results']).label(f'{foreign_table_info.name}_columns')
            )
            .group_by(json_subquery_id_column)
        )
        foreign_json_preselect = foreign_json_preselect.cte(cte_name)

        onclause = get_cte_column(foreign_json_preselect, endpoint_relating_column.name) == endpoint_relationship.local_column_info.db_column
        preselect_columns = [foreign_json_preselect.c[f'{foreign_table_info.name}_columns'].label(f'{foreign_table_info.name}_columns')]
        preselect_join = {"target": foreign_json_preselect, "onclause": onclause}

    log.debug(f"Finished building {cte_name}")
    return preselect_columns, [preselect_join]



# Gets the total distinct counts of a column as a subquery
def column_distinct_count_subquery(db, column):
    return db.query(func.count(distinct(column))).scalar_subquery()

def foreign_table_distinct_count(db, endpoint_preselect, endpoint_table_info, foreign_table_info):
    table_relationship = endpoint_table_info.get_table_relationship(foreign_table_info)
    if table_relationship.requires_mapping_table:
        column_to_count = table_relationship.foreign_mapping_column_info.db_column
        column_to_filter = table_relationship.local_mapping_column_info.db_column
    else:
        column_to_count = table_relationship.foreign_column_info.db_column
        column_to_filter = table_relationship.local_column_info


    # Get count subquery
    entity_count_select = (
            db.query(func.count(distinct(column_to_count)).label("count_result"))
            .filter(column_to_filter.in_(endpoint_preselect))
            .scalar_subquery()
    )
    return entity_count_select


# Gets statistics of a row for numeric columns
def numeric_summary(db, column):
    column_subquery = db.query(
        func.min(column).label("min"),
        func.max(column).label("max"),
        func.round(func.avg(column)).label("mean"),
        func.percentile_disc(0.5).within_group(column).label("median"),
        func.percentile_disc(0.25).within_group(column).label("lower_quartile"),
        func.percentile_disc(0.75).within_group(column).label("upper_quartile"),
    ).subquery("subquery")
    # Get the row_to_json of the subquery
    column_json = db.query(func.row_to_json(column_subquery.table_valued())
                           .label(f"{column.name}_stats")).cte(f"json_{column.name}")
    # Apply an array aggregation
    numeric_array_agg = db.query(func.array_agg(get_cte_column(column_json, f"{column.name}_stats"))).scalar_subquery()
    return numeric_array_agg


# Gets the categorical(grouped) json counts of a row
def basic_categorical_summary(db, column):
    column_preselect = db.query(column, func.count().label("count_result")).group_by(column).subquery("subquery")
    # Get the row_to_json of the subquery
    column_json = db.query(func.row_to_json(column_preselect.table_valued())
                           .label(f"{column.name}_categories")).cte(f"json_{column.name}")
    # Apply an array aggregation
    categorical_array_agg = db.query(func.array_agg(get_cte_column(column_json, f"{column.name}_categories"))).scalar_subquery()
    return categorical_array_agg

def null_aware_categorical_summary(db, db_column, connecting_column):
    non_null_cte = db.query(db_column, connecting_column) \
                    .filter(db_column.is_not(None)) \
                    .group_by(connecting_column, db_column) \
                    .cte(f'{db_column.name}_non_nulls')
    null_cte = db.query(db_column, connecting_column) \
                    .filter(connecting_column.not_in(db.query(get_cte_column(non_null_cte, connecting_column.name)))) \
                    .group_by(connecting_column, db_column) \
                    .cte(f'{db_column.name}_nulls')
    union_subquery = union_all(db.query(non_null_cte.c), db.query(null_cte.c)).set_label_style(SelectLabelStyle.LABEL_STYLE_NONE).subquery(f'{db_column.name}_union')
    union_column = get_cte_column(union_subquery, db_column.name)
    count_subquery = db.query(union_column,func.count().label('count_result')) \
                        .group_by(union_column) \
                        .subquery(f'{db_column.name}_count_subquery') 

    categorical_array_agg = db.query(
                        func.array_agg(
                            func.row_to_json(count_subquery.table_valued().label(f'{db_column.name}_categories'))
                        ).label(f'{db_column.name}_array_agg')).scalar_subquery()
    return categorical_array_agg

# Gets all combinations of data_source columns
def get_data_source_combinations(data_source_columnnames):
    data_source_combinations = {}
    subsets = itertools.chain(
        *map(lambda x: itertools.combinations(data_source_columnnames, x), range(0, len(data_source_columnnames) + 1))
    )
    for subset in subsets:
        if len(subset) > 0:
            combo_boolean = {}
            name = ""
            for columnname in data_source_columnnames:
                boolean = bool(columnname in subset)
                combo_boolean[columnname] = boolean
            name = "_".join([name.split("_")[-1] for name, b in combo_boolean.items() if b])
            if len(subset) < len(data_source_columnnames):
                name += "_exclusive"
            data_source_combinations[name] = combo_boolean
    return data_source_combinations


# Combines the counts of data source columns into a single json for use in summary endpoint
def data_source_counts(db, data_source_columns):
    data_source_counts = []
    data_source_columnnames = [column.name for column in data_source_columns]
    # Get mapping of all combinations of the data source columns
    data_source_combinations = get_data_source_combinations(data_source_columnnames)
    # Get list of queries for each combination of the data_source columns
    for name, data_source_boolean_map in data_source_combinations.items():
        filters = [
            data_source_column == data_source_boolean_map[data_source_column.name]
            for data_source_column in data_source_columns
        ]
        data_source_counts.append(db.query(func.count()).filter(*filters).label(name))

    data_source_preselect = db.query(*data_source_counts).subquery("subquery")
    # Get the row_to_json of the subquery
    data_source_json = db.query(func.row_to_json(data_source_preselect.table_valued()))
    return data_source_json