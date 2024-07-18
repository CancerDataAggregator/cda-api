from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db, columns_query
from cda_api.models import QNode, ColumnResponseObj
from sqlalchemy.orm import Session
from cda_api import get_logger
log = get_logger()


router = APIRouter(
    prefix="/columns",
    tags=["columns"]
)

@router.post('/')
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
        # TODO - possibly a better exception to throw
        raise HTTPException(status_code=404, detail=e)
    return result 