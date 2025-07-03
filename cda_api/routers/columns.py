from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from cda_api import get_logger, get_query_id
from cda_api.application_functions import handle_router_errors
from cda_api.db import get_db
from cda_api.db.query_builders import columns_query
from cda_api.classes.models import ColumnResponseObj

log = get_logger()


router = APIRouter(prefix="/columns", tags=["columns"])


@router.get("/")
def columns_endpoint(request: Request, db: Session = Depends(get_db)) -> ColumnResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        ColumnResponseObj: _description_
    """
    qid = get_query_id()
    log = get_logger(qid)
    try:
        result = columns_query(db, log)
    except Exception as e:
        handle_router_errors(e, log)
    return result
