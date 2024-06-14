from fastapi import FastAPI, Request
from .routers import data, summary, frequencies, release_metadata
from . import get_logger
from cda_api import get_logger

# Establish FastAPI "app" used for decorators on api endpoint functions
app = FastAPI()

# Set up logger
log = get_logger()


# Include all routers
app.include_router(router=data.router)
app.include_router(router=summary.router)
app.include_router(router=frequencies.router)
app.include_router(router=release_metadata.router)


# Temporary Example API Endpoint
@app.get('/hello')
def hello_world(request: Request):
    result = {"result": "hello world"}
    log.info(f'hit hello world endpoint {request.client}')
    return result
