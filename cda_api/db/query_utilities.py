import itertools

import sqlparse
from sqlalchemy import CTE, Label, and_, distinct, func, or_, SelectLabelStyle, union_all, union
from sqlalchemy.exc import CompileError
from sqlalchemy.dialects import postgresql

from cda_api import RelationshipNotFound, get_logger, TableNotFound
from cda_api.db import DB_MAP

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


# distinct(count COLUMN)
def distinct_count(column):
    return func.count(distinct(column))

def normalize_add_exclude_columns(request_body, endpoint_type, filter_columnnames):
    new_add_columns = filter_columnnames
    if request_body.ADD_COLUMNS != None:
        for columnname in request_body.ADD_COLUMNS:
            if columnname.endswith('.*'):
                tablename = columnname.replace('.*', '')
                if endpoint_type == 'data':
                    table_columnnames = [col_info.uniquename for col_info in DB_MAP.get_table_data_column_infos(tablename) if col_info.uniquename not in new_add_columns]
                elif endpoint_type == 'summary':
                    table_columnnames = [col_info.uniquename for col_info in DB_MAP.get_table_summary_column_infos(tablename) if col_info.uniquename not in new_add_columns]
                else:
                    raise Exception(f'Unknown endpoint type: "{endpoint_type}". Could not build column list for table: "{tablename}"')
                new_add_columns = new_add_columns + table_columnnames
            elif columnname not in new_add_columns:
                new_add_columns.append(columnname)

    new_exclude_columns = []
    if request_body.ADD_COLUMNS != None:
        for columnname in request_body.EXCLUDE_COLUMNS:
            if columnname.endswith('.*'):
                tablename = columnname.replace('.*', '')
                if endpoint_type == 'data':
                    table_columnnames = [col_info.uniquename for col_info in DB_MAP.get_table_data_column_infos(tablename) if col_info.uniquename not in new_exclude_columns]
                else:
                    table_columnnames = [col_info.uniquename for col_info in DB_MAP.get_table_summary_column_infos(tablename) if col_info.uniquename not in new_exclude_columns]
                new_exclude_columns = new_exclude_columns + table_columnnames
            elif columnname not in new_exclude_columns:
                new_exclude_columns.append(columnname)

    request_body.ADD_COLUMNS = new_add_columns
    request_body.EXCLUDE_COLUMNS = new_exclude_columns

    return request_body

def apply_match_all_some_filters(query, match_all_filters, match_some_filters):
    # Apply filter conditionals
    if match_all_filters and match_some_filters:
        query = query.filter(and_(*match_all_filters)).filter(or_(*match_some_filters))
    elif match_all_filters:
        query = query.filter(and_(*match_all_filters))
    elif match_some_filters:
        query = query.filter(or_(*match_some_filters))
    return query


# Gets the total distinct counts of a column as a subquery
def total_column_count_subquery(db, preselect):
    return db.query(func.count()).select_from(preselect).scalar_subquery()
    # return db.query(distinct_count(column).label("count_result")).scalar_subquery()


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
def categorical_summary(db, column):
    column_preselect = db.query(column, func.count().label("count_result")).group_by(column).subquery("subquery")
    # Get the row_to_json of the subquery
    column_json = db.query(func.row_to_json(column_preselect.table_valued())
                           .label(f"{column.name}_categories")).cte(f"json_{column.name}")
    # Apply an array aggregation
    categorical_array_agg = db.query(func.array_agg(get_cte_column(column_json, f"{column.name}_categories"))).scalar_subquery()
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
def data_source_counts(db, data_source_columnnames, data_source_columns):
    data_source_counts = []
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

def get_entity_count_components(db, endpoint_tablename, preselect_query, entity_to_count):
    entity_relationship = DB_MAP.get_relationship(endpoint_tablename, entity_to_count)
    entity_local_column = entity_relationship.entity_column
    entity_local_column_uniquename = DB_MAP.get_column_uniquename(
        entity_local_column.name, entity_local_column.table.name
    )

    # Get subquery of the entity_local_column
    column_uniquename = entity_local_column_uniquename
    subquery = db.query(
            get_cte_column(preselect_query, column_uniquename)
            .label(entity_local_column_uniquename)
        )

    # If there is a mapping_table (ie. file_describes_subject)
    if entity_relationship.has_mapping_table:
        column_to_count = entity_relationship.foreign_mapping_column
        column_to_filter_on = entity_relationship.entity_mapping_column
    else:
        column_to_count = entity_local_column
        column_to_filter_on = entity_local_column 
    return subquery, column_to_count, column_to_filter_on

# Gets the total count of an entity's related files and subjects by only counting from the mapping table (ie. observation_of_subject)
def entity_count(db, endpoint_tablename, preselect_query, entity_to_count, filter_table_map):
    subquery, column_to_count, column_to_filter_on = get_entity_count_components(db, endpoint_tablename, preselect_query, entity_to_count)

    # Get count subquery
    entity_count_select = (
            db.query(func.count(distinct(column_to_count)).label("count_result"))
            .filter(column_to_filter_on.in_(subquery))
            .scalar_subquery()
    )
    return entity_count_select

# Get array aggregate of unique values in a column not including null
def unique_column_array_agg(column):
    return func.array_remove(func.array_agg(distinct(column)), None).label(column.name)

def get_foreign_array_columns_and_join(endpoint_tablename, foreign_tablename):
    foreign_array_join = None
    # If there is direct relationship defined
    if DB_MAP.relationship_exists(endpoint_tablename, foreign_tablename):
        relation = DB_MAP.get_relationship(endpoint_tablename, foreign_tablename)
        entity_column = relation.entity_column
        # If there is a mapping table (ie. file_describes subject)
        if relation.has_mapping_table:
            foreign_column = relation.entity_mapping_column
            foreign_array_join = {
                'target': relation.mapping_table, 
                'onclause': relation.foreign_column == relation.foreign_mapping_column
                }

        # If there is a direct relationship (ie. observation -> subject)
        else:
            foreign_column = relation.foreign_column

    # If there is a "hanging table" join defined, (file_tumor_vs_normal is a "hanging table" to file and there is a join defined to subject through file_describes_subject)
    elif DB_MAP.hanging_table_join_exists(foreign_tablename, endpoint_tablename):
        hanging_table_join = DB_MAP.get_hanging_table_join(
            hanging_tablename=foreign_tablename, local_tablename=endpoint_tablename
        )
        # If there is a mapping table involved (ie. file_tumor_vs_normal -> "file_describes_subject" -> subject)
        if "entity_mapping_column" in hanging_table_join.keys():
            entity_column = hanging_table_join["local_column"]
            foreign_column = hanging_table_join["local_mapping_column"]
            foreign_array_join = {
                'target': hanging_table_join["join_table"], 
                'onclause': hanging_table_join["statement"]
                }
            
        # If there is a direct connection (ie. file_tumor_vs_normal -> file)
        else:
            if foreign_tablename == 'upstream_identifiers':
                entity_column = DB_MAP.get_meta_column(f'{endpoint_tablename}_id_alias')
                foreign_column = hanging_table_join["hanging_fk_parent"]
                # foreign_array_join = {
                #     'target': hanging_table_join["join_table"], 
                #     'onclause': entity_column == foreign_column
                #     }
            else:
                entity_column = list(hanging_table_join["join_table"].foreign_keys)[0].column
                foreign_column = hanging_table_join["hanging_fk_parent"]

    else:
        error_message = f'Unable to build foreign array preselect between {endpoint_tablename} and {foreign_tablename}'
        raise RelationshipNotFound(error_message)
    return entity_column, foreign_column, foreign_array_join


# Build the preselects of arrays when adding foreign columns
def build_foreign_array_preselect(db, endpoint_tablename, foreign_tablename, columns, preselect_query, filter_table_map, log):
    cte_name = f"{foreign_tablename}_{endpoint_tablename}_columns"
    entity_column, foreign_column, foreign_array_join = get_foreign_array_columns_and_join(endpoint_tablename, foreign_tablename)
    
    select_cols = [unique_column_array_agg(column).label(column.name) for column in columns] + [foreign_column]
    foreign_array_preselect = (
                    db.query(*select_cols)
                    .filter(foreign_column.in_(preselect_query))
                    .group_by(foreign_column)
                )
    if foreign_tablename in filter_table_map.keys():
        match_all = filter_table_map[foreign_tablename]['match_all']
        match_some = filter_table_map[foreign_tablename]['match_some']
        foreign_array_preselect = apply_match_all_some_filters(foreign_array_preselect, match_all, match_some)
    
    if foreign_tablename == 'upstream_identifiers':
        foreign_array_preselect = foreign_array_preselect.filter(DB_MAP.get_meta_column('upstream_identifiers_cda_table') == endpoint_tablename)

    if foreign_array_join:
        foreign_array_preselect = foreign_array_preselect.join(**foreign_array_join)
    foreign_array_preselect = foreign_array_preselect.cte(cte_name)
    
    onclause = get_cte_column(foreign_array_preselect, foreign_column.name) == entity_column
    preselect_columns = [col for col in foreign_array_preselect.c if col.name != foreign_column.name]
    foreign_join = {"target": foreign_array_preselect, "onclause": onclause}
    return foreign_join, preselect_columns

def build_foreign_json_preselect(db, entity_tablename, foreign_tablename, columns, filter_preselect_query, filter_table_map, log):
    cte_name = f"{foreign_tablename}_expanded_preselect"
    entity_column, entity_mapping_column, foreign_column, foreign_mapping_column, foreign_array_join = get_foreign_array_summary_columns(entity_tablename, foreign_tablename)
    foreign_column_info = DB_MAP.get_table_column_info(foreign_tablename, foreign_column.name)
    
    if (not isinstance(entity_mapping_column, type(None))) and (not isinstance(foreign_mapping_column, type(None))):
        foreign_id_common_name = entity_mapping_column.name
        foreign_table_filter = foreign_column.in_(db.query(foreign_mapping_column).filter(entity_mapping_column.in_(filter_preselect_query)))
        foreign_table_columns = [entity_mapping_column.label(entity_mapping_column.name)] + columns
        foreign_table_subquery = (
            db.query(*foreign_table_columns)
            .filter(foreign_table_filter)
        )
        if foreign_array_join:
            foreign_table_subquery = foreign_table_subquery.join(**foreign_array_join)
    else:
        foreign_id_common_name = foreign_column_info.columnname
        foreign_table_columns = [foreign_column.label(foreign_id_common_name)] + columns
        foreign_table_subquery = (
            db.query(*foreign_table_columns)
            .filter(foreign_column.in_(filter_preselect_query))
        )

    if foreign_tablename in filter_table_map.keys():
        match_all = filter_table_map[foreign_tablename]['match_all']
        match_some = filter_table_map[foreign_tablename]['match_some']
        foreign_table_subquery = apply_match_all_some_filters(foreign_table_subquery, match_all, match_some)

    foreign_table_subquery = foreign_table_subquery.subquery('subquery')
    
    if foreign_tablename == 'foreign_json_columns':
        foreign_table_subquery = foreign_table_subquery.filter(DB_MAP.get_meta_column('upstream_identifiers_cda_table') == entity_tablename)

    foreign_json_columns = []
    subquery_id_column = None
    for column in foreign_table_subquery.c:
        if column.name != foreign_id_common_name:
            foreign_json_columns.append(column.name)
            foreign_json_columns.append(column)
        else:
            subquery_id_column = column
    
    json_subquery = db.query(
                        subquery_id_column.label(foreign_id_common_name),
                        func.json_build_object(*foreign_json_columns).label('json_results')
                    ).subquery('json_subquery')
    
    json_subquery_id_column = json_subquery.c[foreign_id_common_name]

    foreign_json_preselect = (
        db.query(
            json_subquery_id_column.label(foreign_id_common_name),
            func.array_agg(json_subquery.c['json_results']).label(f'{foreign_tablename}_columns')
        )
        .group_by(json_subquery_id_column)
    )
    foreign_json_preselect = foreign_json_preselect.cte(cte_name)

    onclause = get_cte_column(foreign_json_preselect, foreign_id_common_name) == entity_column
    preselect_columns = [foreign_json_preselect.c[f'{foreign_tablename}_columns'].label(f'{foreign_tablename}_columns')]
    foreign_join = {"target": foreign_json_preselect, "onclause": onclause}
    return foreign_join, preselect_columns


def get_foreign_array_summary_subquery(db, entity_tablename, foreign_tablename, select_cols, filter_preselect):
    entity_column, foreign_column, foreign_array_join = get_foreign_array_columns_and_join(entity_tablename, foreign_tablename)
    entity_uniquename = DB_MAP.get_column_uniquename(entity_column.name, entity_column.table.name)
    preselect_column = get_cte_column(filter_preselect, entity_uniquename)
    filter_array_summary_query = db.query(*select_cols).filter(foreign_column.in_(db.query(preselect_column)))

    if foreign_array_join: 
        filter_array_summary_query = filter_array_summary_query.join(**foreign_array_join)
    return filter_array_summary_query.subquery(f"{foreign_tablename}_subquery")

def get_foreign_array_summary_select_columns(db, columns_to_aggregate):
    select_columns = []
    for column in columns_to_aggregate:
        column_count_subquery = (
            db.query(column, func.count().label("count_result"))
            .group_by(column)
            .subquery(f"{column.name}_count_subquery")
            )
        column_array = db.query(
            func.array_agg(
                func.row_to_json(
                    column_count_subquery.table_valued()
                    ).label(f'{column.name}_json')
                ).label(f'{column.name}_array_agg')
            ).scalar_subquery()
        select_columns.append(column_array.label(column.name))
    return select_columns


# def build_foreign_array_summary_preselect(db, entity_tablename, foreign_tablename, columns, preselect_query):
#     foreign_array_subquery = get_foreign_array_summary_subquery(db, entity_tablename, foreign_tablename, columns, preselect_query)

#     select_columns = get_foreign_array_summary_select_columns(db, foreign_array_subquery.columns)

#     foreign_array_preselect = db.query(*select_columns).cte(f'{foreign_tablename}_columns')
#     preselect_columns = [col for col in foreign_array_preselect.c]
    
#     return foreign_array_preselect, preselect_columns
def get_foreign_array_summary_columns(entity_tablename, foreign_tablename):
    foreign_mapping_column = None
    entity_mapping_column = None
    foreign_array_join = None
    # If there is direct relationship defined
    if DB_MAP.relationship_exists(entity_tablename, foreign_tablename):
        relation = DB_MAP.get_relationship(entity_tablename, foreign_tablename)
        entity_column = relation.entity_column
        entity_mapping_column = relation.entity_mapping_column
        foreign_column = relation.foreign_column
        foreign_mapping_column = relation.foreign_mapping_column
        if relation.has_mapping_table:
            foreign_array_join = {
                'target': relation.mapping_table, 
                'onclause': relation.foreign_column == relation.foreign_mapping_column
                }

    # If there is a "hanging table" join defined, 
    # (file_tumor_vs_normal is a "hanging table" to file and there is a join defined to subject through file_describes_subject)
    elif DB_MAP.hanging_table_join_exists(foreign_tablename, entity_tablename):
        hanging_table_join = DB_MAP.get_hanging_table_join(
            hanging_tablename=foreign_tablename, local_tablename=entity_tablename
        )
        # If there is a mapping table involved (ie. file_tumor_vs_normal -> "file_describes_subject" -> subject)
        if "local_mapping_column" in hanging_table_join.keys():
            entity_column = hanging_table_join["local_column"]
            entity_mapping_column = hanging_table_join["local_mapping_column"]
            foreign_column = hanging_table_join["hanging_fk_parent"]
            foreign_mapping_column = hanging_table_join["foreign_mapping_column"]
            foreign_array_join = {
                'target': hanging_table_join["join_table"], 
                'onclause': hanging_table_join["statement"]
                }
        # If there is a direct connection (ie. file_tumor_vs_normal -> file)
        else:
            if foreign_tablename == 'upstream_identifiers':
                entity_column = DB_MAP.get_meta_column(f'{entity_tablename}_id_alias')
                foreign_column = hanging_table_join["hanging_fk_parent"]
            else:
                entity_column = list(hanging_table_join["join_table"].foreign_keys)[0].column
                foreign_column = hanging_table_join["hanging_fk_parent"]

    else:
        error_message = f'Unable to build foreign array preselect between {entity_tablename} and {foreign_tablename}'
        raise RelationshipNotFound(error_message)
    return entity_column, entity_mapping_column, foreign_column, foreign_mapping_column, foreign_array_join
    
def foreign_column_categorical_summary(db, preselect_column, foreign_preselect_id):
    columnname = preselect_column.name
    foreign_id_common_name = foreign_preselect_id.name
    non_null_cte = db.query(preselect_column.label(columnname), foreign_preselect_id.label(foreign_id_common_name)) \
                    .filter(preselect_column.is_not(None)) \
                    .group_by(foreign_preselect_id, preselect_column) \
                    .cte(f'{columnname}_non_nulls')
    null_cte = db.query(preselect_column.label(columnname), foreign_preselect_id.label(foreign_id_common_name)) \
            .filter(foreign_preselect_id.not_in(db.query(get_cte_column(non_null_cte, foreign_id_common_name)))) \
            .group_by(foreign_preselect_id, preselect_column) \
            .cte(f'{columnname}_nulls')
    union_subquery = union_all(db.query(non_null_cte.c), db.query(null_cte.c)).set_label_style(SelectLabelStyle.LABEL_STYLE_NONE).subquery(f'{columnname}_union')


    union_column = get_cte_column(union_subquery, columnname)
    count_subquery = db.query(union_column,func.count().label('count_result')) \
                        .group_by(union_column) \
                        .subquery(f'{columnname}_count_subquery') 

    column_select = db.query(
                        func.array_agg(
                            func.row_to_json(count_subquery.table_valued())
                        ).label(f'{columnname}_array_agg')).scalar_subquery()
    return column_select


# Gets statistics of a row for numeric columns
def foreign_column_numeric_summary(db, preselect_column):
    column_subquery = db.query(
        func.min(preselect_column).label("min"),
        func.max(preselect_column).label("max"),
        func.round(func.avg(preselect_column)).label("mean"),
        func.percentile_disc(0.5).within_group(preselect_column).label("median"),
        func.percentile_disc(0.25).within_group(preselect_column).label("lower_quartile"),
        func.percentile_disc(0.75).within_group(preselect_column).label("upper_quartile"),
    ).subquery("subquery")
    # Get the row_to_json of the subquery
    column_json = db.query(func.row_to_json(column_subquery.table_valued())
                           .label(f"{preselect_column.name}_stats")).cte(f"json_{preselect_column.name}")
    # Apply an array aggregation
    numeric_array_agg = db.query(func.array_agg(get_cte_column(column_json, f"{preselect_column.name}_stats"))).scalar_subquery()
    return numeric_array_agg


def build_foreign_array_summary_preselect(db, endpoint_tablename, foreign_tablename, columns, preselect_query, filter_table_map):
    entity_column, entity_mapping_column, foreign_column, foreign_mapping_column, _= get_foreign_array_summary_columns(endpoint_tablename, foreign_tablename)
    entity_column_info = DB_MAP.get_table_column_info(endpoint_tablename, entity_column.name)
    foreign_column_info = DB_MAP.get_table_column_info(foreign_tablename, foreign_column.name)
    foreign_id_common_name = foreign_column_info.columnname
    if foreign_id_common_name == 'id_alias':
        foreign_id_common_name = foreign_mapping_column.name
    preselect_entity_column = db.query(get_cte_column(preselect_query, entity_column_info.uniquename).label(foreign_id_common_name))
    if (not isinstance(entity_mapping_column, type(None))) and (not isinstance(foreign_mapping_column, type(None))):
        foreign_table_id_cte = db.query(foreign_mapping_column) \
                                .filter(entity_mapping_column.in_(preselect_entity_column))
    else:
        foreign_table_id_cte = preselect_entity_column
    foreign_table_id_cte = foreign_table_id_cte.cte(f'{foreign_tablename}_mapping_ids')
    foreign_table_id_cte_column = db.query(get_cte_column(foreign_table_id_cte, foreign_id_common_name))
    
    foreign_columns = [foreign_column.label(foreign_id_common_name)] + [DB_MAP.get_column_info(column.name).labeled_column for column in columns]
    foreign_table_preselect = db.query(*foreign_columns).filter(foreign_column.in_(foreign_table_id_cte_column))

    if foreign_tablename in filter_table_map.keys():
        print(filter_table_map)
        match_all = filter_table_map[foreign_tablename]['match_all']
        match_some = filter_table_map[foreign_tablename]['match_some']
        foreign_table_preselect = apply_match_all_some_filters(foreign_table_preselect, match_all, match_some)
    
    foreign_table_preselect = foreign_table_preselect.cte(f'{foreign_tablename}_preselect')
    foreign_preselect_id = get_cte_column(foreign_table_preselect, foreign_id_common_name)

    # Get file or subject count
    if endpoint_tablename != 'subject':
        entity_to_count = 'subject'
    else:
        entity_to_count = 'file'
    
    print(f'ft: {foreign_tablename}, etc: {entity_to_count}')
    entity_total_count_select = None
    if foreign_tablename == entity_to_count:
        print('counting?????')
        entity_total_count_select = db.query(func.count(distinct(foreign_preselect_id))).scalar_subquery().label(f'{entity_to_count}_count')
    
    print(entity_total_count_select)
    foreign_selects = []
    for column in columns:
        column_info = DB_MAP.get_column_info(column.name)
        preselect_column = get_cte_column(foreign_table_preselect, column.name)
        if column_info.process_before_display != 'data_source':  
            match column_info.column_type:
                case 'numeric':
                    column_summary = foreign_column_numeric_summary(db, preselect_column)
                    foreign_selects.append(column_summary.label(column_info.uniquename))
                case 'categorical':
                    column_summary = foreign_column_categorical_summary(db, preselect_column, foreign_preselect_id)
                    foreign_selects.append(column_summary.label(column_info.uniquename))
                case _:
                    log.warning(f'Unexpectedly skipping {column_info.column_name} for summary - column_type: {column_info.column_type}')
                    pass

    foreign_array_preselect = db.query(*foreign_selects).cte(f'{foreign_tablename}_columns')
    return foreign_array_preselect.c, entity_total_count_select
    
def get_foreign_array_summary_selects(db, endpoint_tablename, add_columns, preselect_query, filter_table_map, log):
    summary_selects = []
    foreign_array_map = {}
    for columnname in add_columns:
        column_info = DB_MAP.get_column_info(columnname)
        if column_info.tablename != endpoint_tablename:
            if column_info.tablename not in foreign_array_map.keys():
                foreign_array_map[column_info.tablename] = [column_info.metadata_column.label(column_info.uniquename)]
            else:
                foreign_array_map[column_info.tablename].append(column_info.metadata_column.label(column_info.uniquename))
    entity_total_count_select = None
    for foreign_tablename, columns in foreign_array_map.items():
        # build_foreign_array_summary_preselect(db, endpoint_tablename, foreign_tablename, columns, preselect_query, filter_table_map)
        preselect_columns, possible_entity_total_count_select = build_foreign_array_summary_preselect(db, endpoint_tablename, foreign_tablename, columns, preselect_query, filter_table_map)
        if not isinstance(possible_entity_total_count_select, type(None)):
            print('FLAGGED')
            entity_total_count_select = possible_entity_total_count_select
        for column in preselect_columns:
            summary_selects.append(db.query(column).label(column.name))
    return summary_selects, entity_total_count_select



def build_filter_preselect(db, endpoint_tablename, match_all_conditions, match_some_conditions):
    # Get the id_alias column
    endpoint_id_alias = DB_MAP.get_meta_column(f"{endpoint_tablename}_id_alias")

    # Set up the CTE preselect by selecting the id_alias column from it
    preselect_cte = db.query(endpoint_id_alias.label("id_alias"))

    preselect_cte = apply_match_all_some_filters(preselect_cte, match_all_conditions, match_some_conditions)

    preselect_cte = preselect_cte.cte(f"{endpoint_tablename}_preselect")
    preselect_query = db.query(preselect_cte.c.id_alias)
    return preselect_query, endpoint_id_alias


# Combines select columns, match conditions, and mapping columns into cohesive query
def build_match_query(db, select_columns, match_all_conditions=None, match_some_conditions=None, mapping_joins=None):
    # Add select columns
    query = db.query(*select_columns)

    # Add filters
    if match_all_conditions and match_some_conditions:
        query = query.filter(and_(*match_all_conditions)).filter(or_(*match_some_conditions))
    elif match_all_conditions:
        query = query.filter(and_(*match_all_conditions))
    elif match_some_conditions:
        query = query.filter(or_(*match_some_conditions))

    # Add joins
    if mapping_joins:
        # Add joins individually since .join(*mapping_columns) doesn't work for some reason
        for mapping_join in mapping_joins:
            query = query.join(**mapping_join)

    return query

def get_hanging_table_join(endpoint_tablename, select_column):
    if isinstance(select_column, Label):
        column_table = select_column.element.table
        if not isinstance(column_table, CTE):
            column_tablename = column_table.name
            if column_tablename in DB_MAP.hanging_table_relationship_map.keys():
                if endpoint_tablename in DB_MAP.hanging_table_relationship_map[column_tablename].keys():
                    hanging_table_join = DB_MAP.get_hanging_table_join(
                        hanging_tablename=column_tablename, local_tablename=endpoint_tablename
                    )
                    return {'target': hanging_table_join["join_table"], 'onclause': hanging_table_join["statement"]}
    return None
    

def add_hanging_table_joins(endpoint_tablename, select_columns, query):
    hanging_tablenames = []
    for column in select_columns:
        if isinstance(column, Label):
            column_table = column.element.table
            if not isinstance(column_table, CTE):
                column_tablename = column_table.name
                if (
                    column_tablename in DB_MAP.hanging_table_relationship_map.keys()
                    and column_tablename not in hanging_tablenames
                ):
                    hanging_tablenames.append(column_table.name)

    for hanging_tablename in hanging_tablenames:
        if endpoint_tablename in DB_MAP.hanging_table_relationship_map[hanging_tablename].keys():
            hanging_table_join = DB_MAP.get_hanging_table_join(
                hanging_tablename=hanging_tablename, local_tablename=endpoint_tablename
            )
            query = query.join(hanging_table_join["join_table"], hanging_table_join["statement"])
        else:
            log.warning(f"Unable to map {hanging_tablename} and {endpoint_tablename}")
    return query


def get_identifiers_preselect_columns(db, entity_tablename, preselect_query):
    ui_cda_table_info = DB_MAP.get_column_info('upstream_identifiers_cda_table')
    ui_id_alias_info = DB_MAP.get_column_info('upstream_identifiers_id_alias')
    ui_data_source_info = DB_MAP.get_column_info('upstream_identifiers_data_source')
    ui_data_source_id_field_name_info = DB_MAP.get_column_info('data_source_id_field_name')
    ui_data_source_id_value_info = DB_MAP.get_column_info('data_source_id_value')
    ui_subquery = (
        db.query(
            ui_id_alias_info.metadata_column.label(ui_id_alias_info.uniquename),
            ui_data_source_info.metadata_column.label('data_source'),
            ui_data_source_id_field_name_info.metadata_column.label(ui_data_source_id_field_name_info.uniquename),
            ui_data_source_id_value_info.metadata_column.label(ui_data_source_id_value_info.uniquename)
        )
        .filter(ui_cda_table_info.metadata_column == entity_tablename)
        .filter(ui_id_alias_info.metadata_column.in_(preselect_query))
        .subquery('subquery')
    )
    ui_json_subquery = (
        db.query(
            ui_subquery.c[ui_id_alias_info.uniquename].label('id_alias'),
            func.json_build_object(
                'data_source', ui_subquery.c['data_source'],
                ui_subquery.c[ui_data_source_id_field_name_info.uniquename], ui_subquery.c[ui_data_source_id_value_info.uniquename]
            ).label('json_results')
        )
        .subquery('json_subquery')
    )
    ui_preselect = (
        db.query(
            ui_json_subquery.c['id_alias'].label('id_alias'),
            func.array_agg(ui_json_subquery.c['json_results']).label(f'{entity_tablename}_identifier')
        )
        .group_by(ui_json_subquery.c['id_alias'])
    )
    ui_preselect = ui_preselect.cte(f'{entity_tablename}_identifiers_preselect')
    preselect_columns = [get_cte_column(ui_preselect, f'{entity_tablename}_identifier')]
    onclause = get_cte_column(ui_preselect, f'id_alias') == DB_MAP.get_meta_column(f'{entity_tablename}_id_alias')
    foreign_join = {"target": ui_preselect, "onclause": onclause}

    return foreign_join, preselect_columns
    