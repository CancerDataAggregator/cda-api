from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db, frequency_query
from cda_api.models import QNode, FrequencyResponseObj
from sqlalchemy.orm import Session
from cda_api import get_logger
log = get_logger()


router = APIRouter(
    prefix="/frequencies",
    tags=["frequencies"]
)

@router.post('/{columnname}')
def column_frequencies_endpoint(request: Request, 
                                columnname: str, 
                                qnode: QNode, 
                                db: Session = Depends(get_db)) -> FrequencyResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        column_name (str): _description_
        qnode (QNode): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        FrequencyResponseObj: _description_
    """

    result = frequency_query(db, columnname=columnname, qnode=qnode)
    return result