from fastapi import FastAPI, Request
from cda_api.routers import data, summary, release_metadata, unique_values, columns
from cda_api import get_logger

# Establish FastAPI "app" used for decorators on api endpoint functions
app = FastAPI()

# Set up logger
log = get_logger('Setup: main.py')


# Include all routers
app.include_router(router=data.router)
app.include_router(router=summary.router)
app.include_router(router=unique_values.router)
app.include_router(router=release_metadata.router)
app.include_router(router=columns.router)

log.debug('API startup complete')
