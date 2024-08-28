from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db
from cda_api.db.query_builders import fetch_rows
from cda_api.models import QNode, PagedResponseObj
from cda_api import get_logger
from sqlalchemy.orm import Session
import uuid



# API router object. Defines /data endpoint options
router = APIRouter(
    prefix="/data",
    tags=["data"]
)



@router.post('/subject')
def subject_fetch_rows_endpoint(request: Request, 
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
    
    qid = str(uuid.uuid4())
    log = get_logger(qid)
    log.info(f'data/subject endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 
    log.info(f'{request.url}')
   
    try:
        # Get paged query result
        result = fetch_rows(db, endpoint_tablename='subject', qnode=qnode, limit=limit, offset=offset, log=log)
        if (offset != None) and (limit != None):
            if result['total_row_count'] > offset+limit:
                next_url = request.url.components.geturl().replace(f'offset={offset}', f'offset={offset+limit}')
                result['next_url'] = next_url
        else:
            result['next_url'] = None
        log.info('Success')
    except Exception as e:
        # TODO - possibly a better exception to throw
        log.error(e)
        raise HTTPException(status_code=404, detail=str(e))
    
    return result


@router.post('/file')
def file_fetch_rows_endpoint(request: Request, 
                           qnode: QNode, 
                           limit: int = 100,
                           offset: int = 0, 
                           db: Session = Depends(get_db)) -> PagedResponseObj:
    """File data endpoint that returns json formatted row data based on input query

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
    qid = str(uuid.uuid4())
    log = get_logger(qid)
    log.info(f'data/file endpoint hit: {request.client}')
    log.info(f'QNode: {qnode.as_string()}') 
    log.info(f'{request.url}')

    try:
        # Get paged query result
        result = fetch_rows(db, endpoint_tablename='file', qnode=qnode, limit=limit, offset=offset)
        if (offset != None) and (limit != None):
            if result['total_row_count'] > offset+limit:
                next_url = request.url.components.geturl().replace(f'offset={offset}', f'offset={offset+limit}')
                result['next_url'] = next_url
        else:
            result['next_url'] = None
        log.info('Success')
    except Exception as e:
        # TODO - possibly a better exception to throw
        log.error(e)
        raise HTTPException(status_code=404, detail=str(e))
    
    return result
