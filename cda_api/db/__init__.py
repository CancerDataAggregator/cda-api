from .connection import session
from .schema import Base
from cda_api.classes.DatabaseMap import DatabaseMap
from cda_api import get_logger

DB_MAP = DatabaseMap(Base)
log = get_logger('Utility: db/__init__.py')

def get_db():
    db = session()
    try:
        log.debug('Creating database session')
        yield db
    finally:
        log.debug('Closing database session')
        db.close()


