from fastapi import Depends, APIRouter, HTTPException, Request
from cda_api.db import get_db, unique_value_query
from cda_api.models import QNode, UniqueValueResponseObj
from sqlalchemy.orm import Session
from cda_api import get_logger
from typing import Optional
log = get_logger()


router = APIRouter(
    prefix="/unique_values",
    tags=["unique_values"]
)

@router.post('/{columnname}')
def unique_values_endpoint(request: Request, 
                            columnname: str, 
                            system: str = '',
                            count: bool = False,
                            totalCount: bool = False,
                            limit: int = None,
                            offset: int = None,
                            db: Session = Depends(get_db)) -> UniqueValueResponseObj:
    """_summary_

    Args:
        request (Request): _description_
        column_name (str): _description_
        qnode (QNode): _description_
        db (Session, optional): _description_. Defaults to Depends(get_db).

    Returns:
        FrequencyResponseObj: _description_
    """
    try:
        # Get paged query result
        result = unique_value_query(db, 
                                columnname=columnname,
                                system=system,
                                countOpt=count,
                                totalCount=totalCount,
                                limit=limit,
                                offset=offset)
        # TODO need to figure out better way to handle limit and offset 
        if offset and limit:
            if result['total_row_count'] > offset+limit:
                next_url = request.url.components.geturl().replace(f'offset={offset}', f'offset={offset+limit}')
                result['next_url'] = next_url
                print('Built next_url')
        else:
            result['next_url'] = None
            print('next_url = None')
        if not totalCount:
            result['total_row_count'] = None
            print('Overrode total_row_count')
    except Exception as e:
        # TODO - possibly a better exception to throw
        raise HTTPException(status_code=404, detail=str(e))
    return result