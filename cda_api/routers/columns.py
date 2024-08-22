from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db
from cda_api.db.query_builders import columns_query
from cda_api.models import QNode, ColumnResponseObj
from sqlalchemy.orm import Session
from cda_api import get_logger
log = get_logger()


router = APIRouter(
    prefix="/columns",
    tags=["columns"]
)

@router.get('/')
def columns_endpoint(request: Request, 
                     db: Session = Depends(get_db)) -> ColumnResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        ColumnResponseObj: _description_
    """

    try:
        result = columns_query(db)
    except Exception as e:
        log.error(e)
        raise HTTPException(status_code=404, detail=str(e))
    return result 