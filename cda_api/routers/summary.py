from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db
from cda_api.db.query_builders import summary_query
from cda_api.models import QNode, SummaryResponseObj
from sqlalchemy.orm import Session
from cda_api import get_logger

log = get_logger()


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
    
    try:
        result = summary_query(db, endpoint_tablename='subject', qnode=qnode)
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
    
    try:
        result = summary_query(db, endpoint_tablename='file', qnode=qnode)
    except Exception as e:
        # TODO - possibly a better exception to throw
        log.exception(str(e))
        raise HTTPException(status_code=404, detail=str(e))
    return result