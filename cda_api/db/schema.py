from sqlalchemy.ext.automap import automap_base


from cda_api import get_logger
from cda_api.db.connection import engine

log = get_logger("Setup: schema.py")

try:
    log.info("Building SQLAlchemy automap")
    Base = automap_base()
    Base.prepare(autoload_with=engine)
    log.info("Successfully built SQLAlchemy automap")
except Exception as e:
    log.exception(e)
    raise e
