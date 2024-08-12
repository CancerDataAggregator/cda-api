from sqlalchemy.ext.automap import automap_base
from cda_api.db import engine
from sqlalchemy import inspect
from sqlalchemy.orm import relationship
from cda_api.classes.DatabaseMap import DatabaseMap
from cda_api import RelationshipError



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


for tablename, table in Base.classes.items():
    current_table_relationships = [r.target.name for r in inspect(table).relationships] + [tablename, 'subject']
    for target_tablename, target_table in Base.classes.items():
        meta_table = Base.metadata.tables[tablename]
        meta_target_table = Base.metadata.tables[target_tablename]
        if target_tablename in current_table_relationships:
            continue
        if ('subject_alias' in meta_table.columns.keys()) and ('subject_alias' in meta_target_table.columns.keys()):
            continue
        elif ('subject_alias' in meta_table.columns.keys()):
            try:
                r = [r for r in inspect(target_table).relationships if r.target.name == 'subject'][0]
                subject_mapping_table = r.secondary
                target_local_column, target_mapping_column = [(local, remote) for (local, remote) in r.local_remote_pairs if local.table.name == target_tablename][0]
            except:
                raise RelationshipError(f'Error mapping between {tablename} and {target_tablename}')
            primary_join = table.subject_alias == subject_mapping_table.columns['subject_alias']
            secondary_join = target_local_column == target_mapping_column
            rel = relationship(target_tablename, 
                               primaryjoin = primary_join,
                               secondaryjoin = secondary_join,
                               secondary = subject_mapping_table,
                               viewonly=True)
        elif ('subject_alias' in meta_target_table.columns.keys()):
            try:
                r = [r for r in inspect(table).relationships if r.target.name == 'subject'][0]
                subject_mapping_table = r.secondary
                local_column, mapping_column = [(local, remote) for (local, remote) in r.local_remote_pairs if local.table.name == tablename][0]
            except:
                raise RelationshipError(f'Error mapping between {tablename} and {target_tablename}')
            primary_join = local_column == mapping_column
            secondary_join = target_table.subject_alias == subject_mapping_table.columns['subject_alias']
            rel = relationship(target_tablename, 
                               primaryjoin = primary_join,
                               secondaryjoin = secondary_join,
                               secondary = subject_mapping_table,
                               viewonly=True)
        else:
            raise RelationshipError(f'Error mapping between {tablename} and {target_tablename}')
        setattr(table, f'{target_tablename}', rel)


def get_db_map() -> DatabaseMap:
    db_map = DatabaseMap(Base)
    return db_map