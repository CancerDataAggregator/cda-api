from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db, paged_query
from cda_api.models import QNode, PagedResponseObj
from cda_api import get_logger
from sqlalchemy.orm import Session

log = get_logger()

# API router object. Defines /data endpoint options
router = APIRouter(
    prefix="/data",
    tags=["data"]
)


@router.post('/')
def default_paged_endpoint(request: Request, 
                           qnode: QNode, 
                           limit: int = 100,
                           offset: int = 0, 
                           db: Session = Depends(get_db)) -> PagedResponseObj:
    """Default data endpoint that returns json formatted row data based on input query

    Args:
        request (Request): HTTP request object
        qnode (QNode): JSON input query
        limit (int, optional): Limit for paged results. Defaults to 100.
        offset (int, optional): Offset for paged results. Defaults to 0.
        db (Session, optional): Database session object. Defaults to Depends(get_db).

    Returns:
        PagedResponseObj: 
        { 
            'result': [{'column': 'data'}], 
            'query_sql': 'SQL statement used to generate result',
            'total_row_count': 'total rows of data for query generated (not paged)',
            'next_url': 'URL to acquire next paged result'
        }
    """

    log.debug(f'default paged endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 

    try:
        # Get paged query result
        result = paged_query(db, endpoint_tablename=None, qnode=qnode, limit=limit, offset=offset)
    except Exception as e:
        # TODO - possibly a better exception to throw
        raise HTTPException(status_code=404, detail=e)
    return result


@router.post('/subject')
def subject_paged_endpoint(request: Request, 
                           qnode: QNode, 
                           limit: int = 100,
                           offset: int = 0,
                           db: Session = Depends(get_db)) -> PagedResponseObj:
    """Subject data endpoint that returns json formatted row data based on input query

    Args:
        request (Request): HTTP request object
        qnode (QNode): JSON input query
        limit (int, optional): Limit for paged results. Defaults to 100.
        offset (int, optional): Offset for paged results. Defaults to 0.
        db (Session, optional): Database session object. Defaults to Depends(get_db).

    Returns:
        PagedResponseObj: 
        { 
            'result': [{'column': 'data'}], 
            'query_sql': 'SQL statement used to generate result',
            'total_row_count': 'total rows of data for query generated (not paged)',
            'next_url': 'URL to acquire next paged result'
        }
    """

    log.debug(f'subject paged endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 
    log.debug(f'{request.url}')
    next_url = request.url.components.geturl().replace(f'offset={offset}', f'offset={offset+limit}')
    log.debug(f'{next_url}')
    try:
        # Get paged query result
        result = paged_query(db, endpoint_tablename='subject', qnode=qnode, limit=limit, offset=offset)
    except Exception as e:
        # TODO - possibly a better exception to throw
        raise HTTPException(status_code=404, detail=str(e))
    return result


@router.post('/observation')
def observation_paged_endpoint(request: Request, 
                           qnode: QNode, 
                           limit: int = 100,
                           offset: int = 0, 
                           db: Session = Depends(get_db)) -> PagedResponseObj:
    """Observation data endpoint that returns json formatted row data based on input query

    Args:
        request (Request): HTTP request object
        qnode (QNode): JSON input query
        limit (int, optional): Limit for paged results. Defaults to 100.
        offset (int, optional): Offset for paged results. Defaults to 0.
        db (Session, optional): Database session object. Defaults to Depends(get_db).

    Returns:
        PagedResponseObj: 
        { 
            'result': [{'column': 'data'}], 
            'query_sql': 'SQL statement used to generate result',
            'total_row_count': 'total rows of data for query generated (not paged)',
            'next_url': 'URL to acquire next paged result'
        }
    """

    log.debug(f'observation paged endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 

    try:
        # Get paged query result
        result = paged_query(db, endpoint_tablename='observation', qnode=qnode, limit=limit, offset=offset)
    except Exception as e:
        # TODO - possibly a better exception to throw
        raise HTTPException(status_code=404, detail=e)
    return result
