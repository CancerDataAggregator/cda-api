from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from cda_api import get_logger, get_query_id
from cda_api.application_functions import handle_router_errors, get_client_ip
from cda_api.db import get_db
from cda_api.db.query_builders import release_metadata_query
from cda_api.classes.models import ReleaseMetadataObj

router = APIRouter(prefix="/release_metadata", tags=["release_metadata"])


# TODO - include count(*) for all tables
@router.get("/")
def release_metadata_endpoint(request: Request, db: Session = Depends(get_db)) -> ReleaseMetadataObj:
    """_summary_

    Args:
        request (Request): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        ReleaseMetadataObj: _description_
    """
    qid = get_query_id()
    log = get_logger(qid)
    client_ip = get_client_ip(request)
    log.info(f"release_metadata endpoint hit: {client_ip}")
    log.info(f"{request.url}")

    try:
        result = release_metadata_query(db, log)
        log.info("Success")
    except Exception as e:
        handle_router_errors(e, log)
    return result
