import logging
import logging.config
import yaml
from sqlalchemy.exc import OperationalError
from fastapi import HTTPException
import uuid

# Function to generate logger from config file
def get_logger(id = '') -> logging.Logger:
    with open('cda_api/config/logger.yml') as log_config_file:
        log_config = yaml.safe_load(log_config_file)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger("simple")
    extra = {'id': id}
    logger = logging.LoggerAdapter(logger, extra)
    return logger


def handle_router_errors(e, log):
    if isinstance(e, OperationalError):
        log.exception(e)
        raise HTTPException(status_code=404, detail='There was a slight drop in the database connection, please attempt your query again.')
    elif isinstance(e, Exception):
        # TODO - possibly a better exception to throw
        log.exception(e)
        raise HTTPException(status_code=404, detail=str(e))
    else:
        log.error(f"Unexpected object passed to error handler: {e}")

def get_query_id():
    return f'Query: {str(uuid.uuid4())}'
    