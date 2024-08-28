from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db
from cda_api.db.query_builders import summary_query
from cda_api.models import QNode, SummaryResponseObj
from sqlalchemy.orm import Session
from cda_api import get_logger
import uuid


router = APIRouter(
    prefix="/summary",
    tags=["summary"]
)

@router.post('/subject')
def subject_summary_endpoint(request: Request, 
                             qnode: QNode, 
                             db: Session = Depends(get_db)) -> SummaryResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        qnode (QNode): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        SummaryResponseObj: _description_
    """
    
    qid = str(uuid.uuid4())
    log = get_logger(qid)
    log.info(f'summary/subject endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 
    log.info(f'{request.url}')
    try:
        result = summary_query(db, endpoint_tablename='subject', qnode=qnode, log=log)
        log.info('Success')
    except Exception as e:
        # TODO - possibly a better exception to throw
        log.exception(str(e))
        raise HTTPException(status_code=404, detail=str(e))
    return result

@router.post('/file')
def file_summary_endpoint(request: Request, 
                             qnode: QNode, 
                             db: Session = Depends(get_db)) -> SummaryResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        qnode (QNode): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        SummaryResponseObj: _description_
    """

    qid = str(uuid.uuid4())
    log = get_logger(qid)
    log.info(f'summary/file endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 
    log.info(f'{request.url}')
    try:
        result = summary_query(db, endpoint_tablename='file', qnode=qnode)
        log.info('Success')
    except Exception as e:
        # TODO - possibly a better exception to throw
        log.exception(str(e))
        raise HTTPException(status_code=404, detail=str(e))
    return result