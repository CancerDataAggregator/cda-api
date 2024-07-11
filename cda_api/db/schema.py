from sqlalchemy.ext.automap import automap_base
from cda_api.db import engine
from sqlalchemy import inspect
from cda_api.classes.ColumnInfo import ColumnInfo
from cda_api.classes.DatabaseMap import DatabaseMap
from cda_api.classes.EntityRelationship import EntityRelationship

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

def get_db_map() -> DatabaseMap:
    db_map = DatabaseMap(Base)
    return db_map