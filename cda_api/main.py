import uvicorn
from fastapi import FastAPI, status, Request
from fastapi.responses import JSONResponse

from cda_api import get_logger, CDABaseException
from cda_api.routers import column_values, columns, data, release_metadata, summary
from cda_api.classes.models import ClientError, InternalError

# Establish FastAPI "app" used for decorators on api endpoint functions
app = FastAPI()


# Set up logger
log = get_logger("Setup: main.py")

# Include all routers
app.include_router(router=data.router,
                   responses={
                        400: {
                            "model": ClientError
                        },
                        500: {
                            "model": InternalError
                        }
                })

app.include_router(router=summary.router,
                   responses={
                        400: {
                            "model": ClientError
                        },
                        500: {
                            "model": InternalError
                        }
                })
app.include_router(router=column_values.router,
                   responses={
                        400: {
                            "model": ClientError
                        },
                        500: {
                            "model": InternalError
                        }
                })
app.include_router(router=release_metadata.router,
                   responses={
                        400: {
                            "model": ClientError
                        },
                        500: {
                            "model": InternalError
                        }
                })
app.include_router(router=columns.router,
                   responses={
                        400: {
                            "model": ClientError
                        },
                        500: {
                            "model": InternalError
                        }
                })

@app.exception_handler(CDABaseException)
def cda_exception_handler(request: Request, exc: CDABaseException):
    """Custom handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content= {'error_type':exc.name, 'message':exc.message}
    )

log.info("API startup complete")


def start_api():
    uvicorn.run("cda_api.main:app", host="0.0.0.0", port=8000, reload=True)
