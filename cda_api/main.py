import uvicorn
from fastapi import FastAPI

from cda_api import get_logger
from cda_api.routers import columns, data, release_metadata, summary, unique_values

# Establish FastAPI "app" used for decorators on api endpoint functions
app = FastAPI()

# Set up logger
log = get_logger("Setup: main.py")


# Include all routers
app.include_router(router=data.router)
app.include_router(router=summary.router)
app.include_router(router=unique_values.router)
app.include_router(router=release_metadata.router)
app.include_router(router=columns.router)

log.debug("API startup complete")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
