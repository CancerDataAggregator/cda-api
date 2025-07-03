import logging
import uuid
from os import getenv

import yaml
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError, ProgrammingError, DataError, ArgumentError
from cda_api.classes.models import ClientError, InternalError
from cda_api.classes.exceptions import CDABaseException, DatabaseConnectionDrop, InternalErrorException, InvalidFilterError


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


def convert_exceptions(e, log):
    if isinstance(e, OperationalError):
        log.debug('Database drop detected. Converting error output')
        log.error(e)
        error = DatabaseConnectionDrop('A drop in the database connection was detected, please attempt your query again.')
    elif isinstance(e, ProgrammingError) and 'operator does not exist' in str(e):
        message = f'Invalid match filter provided. Please verify that you are using the correct operators and values for the column used in the filter'
        log.debug('Invalid operator detected. Converting error output')
        error = InvalidFilterError(message)
    elif isinstance(e, ProgrammingError) and 'function upper(bigint) does not exist' in str(e):
        message = f'Invalid match filter provided. Please verify that you are using the correct operators and values for the column used in the filter'
        log.debug('Invalid operator detected. Converting error output')
        error = InvalidFilterError(message)
    elif isinstance(e, ProgrammingError) and 'function upper(boolean) does not exist' in str(e):
        message = f'Invalid match filter provided. Please verify that you are using the correct operators and values for the column used in the filter'
        log.debug('Invalid operator detected. Converting error output')
        error = InvalidFilterError(message)
    elif isinstance(e, ProgrammingError) and 'must be type boolean' in str(e):
        message = f'Invalid match filter provided. Please verify that you are using the correct operators and values for the column used in the filter'
        log.debug('Invalid operator detected. Converting error output')
        error = InvalidFilterError(message)
    elif isinstance(e, DataError) and 'invalid input syntax for type' in str(e):
        message = f'Invalid match filter provided. Please verify that you are using the correct operators and values for the column used in the filter'
        log.debug('Invalid operator detected. Converting error output')
        error = InvalidFilterError(message)
    elif isinstance(e, ArgumentError) and 'operators can be used with None/True/False' in str(e):
        message = f'Invalid match filter provided. Please verify that you are using the correct operators and values for the column used in the filter'
        log.debug('Invalid operator detected. Converting error output')
        error = InvalidFilterError(message)
    else:
        # default to server error
        log.debug('Unexpected error detected. Converting error output')
        error = InternalErrorException(str(e))
    return error
    

def handle_router_errors(e, log):
    log.error(f'Error of type: {type(e)} caught')
    log.error(e)
    if not isinstance(e, CDABaseException):
        e = convert_exceptions(e, log)
    raise(e)

def get_query_id():
    return f"Query: {str(uuid.uuid4())}"
