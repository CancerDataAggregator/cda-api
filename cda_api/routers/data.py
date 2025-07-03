from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from cda_api import EmptyQueryError, get_logger, get_query_id
from cda_api.application_functions import handle_router_errors
from cda_api.db import get_db
from cda_api.db.query_builders import data_query
from cda_api.classes.models import PagedResponseObj, DataRequestBody

# API router object. Defines /data endpoint options
router = APIRouter(prefix="/data", tags=["data"])


@router.post("/file")
def file_fetch_rows_endpoint(
    request: Request, request_body: DataRequestBody, limit: int = 100, offset: int = 0, db: Session = Depends(get_db)
) -> PagedResponseObj:
    """File data endpoint that returns json formatted row data based on input query

    Args:
        request (Request): HTTP request object
        request_body (DataRequestBody): JSON input query
        limit (int, optional): Limit for paged results. Defaults to 100.
        offset (int, optional): Offset for paged results. Defaults to 0.
        db (Session, optional): Database session object. Defaults to Depends(get_db).

    Returns:
        PagedResponseObj:
        {
            'result': [{'column': 'data'}],
            'query_sql': 'SQL statement used to generate result',
            'total_row_count': 'total rows of data for query generated (not paged)',
            'next_url': 'URL to acquire next paged result'
        }
    """
    qid = get_query_id()
    log = get_logger(qid)

    log.info(f"data/file endpoint hit: {request.client}")
    log.info(f"DataRequestBody: {request_body.as_string()}")
    log.info(f"{request.url}")

    try:
        # Get paged query result
        result = data_query(db, endpoint_table_name="file", request_body=request_body, limit=limit, offset=offset, log=log)
        if (offset != None) and (limit != None):
            if result["total_row_count"] > offset + limit:
                next_url = request.url.components.geturl().replace(f"offset={offset}", f"offset={offset+limit}")
                result["next_url"] = next_url
        else:
            result["next_url"] = None
        log.info("Success")
    except Exception as e:
        handle_router_errors(e, log)

    return result


@router.post("/subject")
def subject_fetch_rows_endpoint(
    request: Request, request_body: DataRequestBody, limit: int = 100, offset: int = 0, db: Session = Depends(get_db)
) -> PagedResponseObj:
    """Subject data endpoint that returns json formatted row data based on input query

    Args:
        request (Request): HTTP request object
        request_body (DataRequestBody): JSON input query
        limit (int, optional): Limit for paged results. Defaults to 100.
        offset (int, optional): Offset for paged results. Defaults to 0.
        db (Session, optional): Database session object. Defaults to Depends(get_db).

    Returns:
        PagedResponseObj:
        {
            'result': [{'column': 'data'}],
            'query_sql': 'SQL statement used to generate result',
            'total_row_count': 'total rows of data for query generated (not paged)',
            'next_url': 'URL to acquire next paged result'
        }
    """

    qid = get_query_id()
    log = get_logger(qid)
    log.info(f"data/subject endpoint hit: {request.client}")
    log.info(f"DataRequestBody: {request_body.as_string()}")
    log.info(f"{request.url}")

    try:
        # Get paged query result
        result = data_query(db, endpoint_table_name="subject", request_body=request_body, limit=limit, offset=offset, log=log)
        if limit != None:
            if offset == None:
                offset = 0
            if result["total_row_count"] > offset + limit:
                next_url = request.url.components.geturl().replace(f"offset={offset}", f"offset={offset+limit}")
                result["next_url"] = next_url
        else:
            result["next_url"] = None
        log.info("Success")
    except Exception as e:
        handle_router_errors(e, log)

    return result