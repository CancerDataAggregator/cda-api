import uvicorn
from fastapi import FastAPI, status, Request
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi


from sqlalchemy.exc import OperationalError


from cda_api import get_logger, CDABaseException
# from cda_api.handlers import database_dropout_handler
from cda_api.application_utilities import database_connection_drop_handler
from cda_api.routers import column_values, columns, data, release_metadata, summary
from cda_api.models import ClientError, InternalError

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
app.include_router(router=column_values.router)
app.include_router(router=release_metadata.router)
app.include_router(router=columns.router)

@app.exception_handler(CDABaseException)
def cda_exception_handler(request: Request, exc: CDABaseException):
    """Custom handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content= {'error_type':exc.name, 'message':exc.message}
    )

# app.add_exception_handler(OperationalError, database_connection_drop_handler)
# app.add_exception_handler(DatabaseDropout, database_dropout_handler)


log.debug("API startup complete")

# def custom_openapi():
#     if app.openapi_schema:
#         return app.openapi_schema
#     openapi_schema = get_openapi(
#         title="Custom title",
#         version="2.5.0",
#         description="This is a very custom OpenAPI schema",
#         routes=app.routes,
#     )
#     for path in openapi_schema["paths"]:
#         for method in openapi_schema["paths"][path]:
#             if openapi_schema["paths"][path][method]["responses"].get("422"):
#                 openapi_schema["paths"][path][method]["responses"][
#                     "400"
#                 ] = openapi_schema["paths"][path][method]["responses"]["422"]
#                 openapi_schema["paths"][path][method]["responses"].pop("422")
#     app.openapi_schema = openapi_schema
#     return app.openapi_schema


# app.openapi = custom_openapi




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
