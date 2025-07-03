from cda_api import get_logger
from cda_api.classes.DatabaseInfo import DatabaseInfo
from .connection import session
from .schema import Base

DB_INFO = DatabaseInfo(Base)
log = get_logger("Utility: db/__init__.py")


def get_db():
    db = session()
    try:
        log.debug("Creating database session")
        yield db
    finally:
        log.debug("Closing database session")
        db.close()
