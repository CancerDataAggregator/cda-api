from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db.metadata import get_release_metadata
from cda_api.db import get_db
from cda_api import get_logger
from cda_api.models import QNode, ReleaseMetadataObj
from sqlalchemy.orm import Session
import uuid


router = APIRouter(
    prefix="/release_metadata",
    tags=["release_metadata"]
)

# TODO - include count(*) for all tables
@router.get('/')
def release_metadata_endpoint(request: Request, 
                              db: Session = Depends(get_db)) -> ReleaseMetadataObj:
    """_summary_

    Args:
        request (Request): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        FrequencyResponseObj: _description_
    """
    qid = str(uuid.uuid4())
    log = get_logger(qid)
    log.info(f'release_metadata endpoint hit: {request.client}')
    log.info(f'{request.url}')
    
    try:
        result = get_release_metadata(db)
        log.info('Success')
    except Exception as e:
        # TODO - possibly a better exception to throw
        log.error(e)
        raise HTTPException(status_code=404, detail=str(e))
    return result