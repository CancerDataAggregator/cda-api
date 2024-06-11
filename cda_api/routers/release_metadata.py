from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db, get_release_metadata
from cda_api.models import QNode, FrequencyResponseObj
from sqlalchemy.orm import Session


router = APIRouter(
    prefix="/release_metadata",
    tags=["release_metadata"]
)

@router.get('/')
def release_metadata_endpoint(request: Request, 
                              db: Session = Depends(get_db)) -> FrequencyResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        FrequencyResponseObj: _description_
    """
    
    try:
        result = get_release_metadata(db)
    except Exception as e:
        # TODO - possibly a better exception to throw
        raise HTTPException(status_code=404, detail=e)
    return result