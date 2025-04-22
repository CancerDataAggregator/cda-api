from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from cda_api import get_logger, get_query_id
from cda_api.application_utilities import handle_router_errors
from cda_api.db import get_db
from cda_api.db.query_builders import column_values_query
from cda_api.models import ColumnValuesResponseObj

router = APIRouter(prefix="/column_values", tags=["column_values"])


@router.post("/{column}")
def column_values_endpoint(
    request: Request,
    column: str,
    data_source: str = "",
    limit: int = None,
    offset: int = None,
    db: Session = Depends(get_db),
) -> ColumnValuesResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        column (str): _description_
        data_source (str): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        ColumnValuesResponseObj: _description_
    """
    qid = get_query_id()
    log = get_logger(qid)
    log.info(f"column_values endpoint hit: {request.client}")
    log.info(f"{request.url}")

    try:
        # Get paged query result
        result = column_values_query(
            db,
            column=column,
            data_source=data_source,
            limit=limit,
            offset=offset,
            log=log,
        )

        # TODO need to figure out better way to handle limit and offset
        result["next_url"] = None
        if (offset != None) and (limit != None):
            if result["total_row_count"] > offset + limit:
                next_url = request.url.components.geturl().replace(f"offset={offset}", f"offset={offset+limit}")
                result["next_url"] = next_url


    except Exception as e:
        handle_router_errors(e, log)
    return result
