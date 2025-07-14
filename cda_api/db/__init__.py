from cda_api import get_logger
from cda_api.classes.DatabaseInfo import DatabaseInfo
from .connection import get_db
from .schema import Base


DB_INFO = DatabaseInfo(Base)
log = get_logger("Utility: db/__init__.py")


