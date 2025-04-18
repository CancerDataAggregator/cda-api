import logging
import uuid
from os import getenv

import yaml
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from cda_api.models import ClientError, InternalError
from cda_api.classes.exceptions import CDABaseException, DatabaseConnectionDrop, InternalErrorException


# Function to generate logger from config file
def get_logger(id="") -> logging.Logger:
    if getenv("DOCKER_DEPLOYED"):
        with open("cda_api/config/docker_logger.yml") as log_config_file:
            log_config = yaml.safe_load(log_config_file)
    else:
        with open("cda_api/config/logger.yml") as log_config_file:
            log_config = yaml.safe_load(log_config_file)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger("simple")
    extra = {"id": id}
    logger = logging.LoggerAdapter(logger, extra)
    return logger



    

def database_connection_drop_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content=InternalError(type='DatabaseConnectionDropped', message=str(exc)),
    )

def convert_exceptions(e, log):
    if isinstance(e, OperationalError):
        log.debug('Database drop detected. Converting error output')
        log.error(e)
        error = DatabaseConnectionDrop('A drop in the database connection was detected, please attempt your query again.')
    else:
        # default to server error
        log.debug('Unexpected error detected. Converting error output')
        error = InternalErrorException(str(e))
    return error
    

def handle_router_errors(e, log):
    log.error(e)
    if not isinstance(e, CDABaseException):
        e = convert_exceptions(e, log)
    raise(e)

def get_query_id():
    return f"Query: {str(uuid.uuid4())}"
