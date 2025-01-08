from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from cda_api import EmptyQueryError, get_logger, get_query_id
from cda_api.application_utilities import handle_router_errors
from cda_api.db import get_db
from cda_api.db.query_builders import summary_query
from cda_api.models import QNode, SummaryResponseObj

router = APIRouter(prefix="/summary", tags=["summary"])


@router.post("/subject")
def subject_summary_endpoint(request: Request, qnode: QNode, db: Session = Depends(get_db)) -> SummaryResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        qnode (QNode): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        SummaryResponseObj: _description_
    """

    qid = get_query_id()
    log = get_logger(qid)
    log.info(f"summary/subject endpoint hit: {request.client}")
    log.info(f"QNode: {qnode.as_string()}")
    log.info(f"{request.url}")
    if qnode.is_empty():
        e = EmptyQueryError("Must provide either/both of 'MATCH_ALL' or 'MATCH_SOME' within the request body")
        log.exception(e)
        raise HTTPException(status_code=404, detail=str(e))

    try:
        result = summary_query(db, endpoint_tablename="subject", qnode=qnode, log=log)
        log.info("Success")
    except Exception as e:
        handle_router_errors(e, log)
    return result


@router.post("/file")
def file_summary_endpoint(request: Request, qnode: QNode, db: Session = Depends(get_db)) -> SummaryResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        qnode (QNode): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        SummaryResponseObj: _description_
    """

    qid = get_query_id()
    log = get_logger(qid)
    log.info(f"summary/file endpoint hit: {request.client}")
    log.info(f"QNode: {qnode.as_string()}")
    log.info(f"{request.url}")
    if qnode.is_empty():
        e = EmptyQueryError("Must provide either/both of 'MATCH_ALL' or 'MATCH_SOME' within the request body")
        log.exception(e)
        raise HTTPException(status_code=404, detail=str(e))

    try:
        result = summary_query(db, endpoint_tablename="file", qnode=qnode, log=log)
        log.info("Success")
    except Exception as e:
        handle_router_errors(e, log)
    return result
