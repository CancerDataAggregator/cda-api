from .connection import session
from .schema import Base
from cda_api.classes.DatabaseMap import DatabaseMap


def get_db():
    db = session()
    try:
        yield db
    finally:
        db.close()

def get_db_map() -> DatabaseMap:
    db_map = DatabaseMap(Base)
    return db_map