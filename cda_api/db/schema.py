from sqlalchemy.ext.automap import automap_base, generate_relationship
from cda_api.db import engine
from sqlalchemy import inspect

# Ran into issue with self referential table relationship collection
# The following resolves the naming issue
def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    disc = '_'.join(col.name for col in constraint.columns)
    return referred_cls.__name__.lower() + '_' + disc + "_collection"


Base = automap_base()
Base.prepare(autoload_with=engine,
             name_for_collection_relationship=name_for_collection_relationship)
TABLE_LIST = Base.classes.values()


# HACK to add all collection attributes to the table classes consistently.
for table in TABLE_LIST:
    i = inspect(table)
    i.relationships
    # print([r for r in i.relationships])


# Building out a mapping of all columns with there unique name as a key with all relevant info we need to pair with it
COLUMN_MAP = {}
def build_column_map():
    entity_table_names = [name for name, _ in Base.classes.items()]
    entity_column_name_list = [column.name 
                               for _, table in Base.metadata.tables.items() 
                               for column in table.columns
                               if table.name != 'release_metadata']
    duplicate_column_names = list(set([column_name 
                                       for column_name in entity_column_name_list 
                                       if entity_column_name_list.count(column_name) > 1]))

    for table_name, table in Base.classes.items():
        meta_table = Base.metadata.tables[table_name]
        if table_name not in entity_table_names:
            continue
        for meta_column in meta_table.columns:
            if meta_column.name in duplicate_column_names:
                unique_name = f'{table_name}_{meta_column.name}'
            else:
                unique_name = meta_column.name
            table_column_name = f'{table_name}.{meta_column.name}'
            # meta_table = 
            COLUMN_MAP[unique_name] = {'TABLE_NAME': table_name,
                                        'TABLE': table,
                                        'META_TABLE': meta_table,
                                        'TABLE_COLUMN_NAME': table_column_name,
                                        'COLUMN_NAME': meta_column.name,
                                        'META_COLUMN': meta_column,
                                        'COLUMN': getattr(table, meta_column.name)}
            

build_column_map()