from cda_api.db.schema import Base
from cda_api.db.query_utilities import query_to_string
from cda_api import get_logger
from sqlalchemy import func

log = get_logger()

def get_release_metadata(db):
    subquery = db.query(Base.metadata.tables['release_metadata']).subquery('subquery')
    query = db.query(func.row_to_json(subquery.table_valued()))
    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    result = query.all()
    result = [row for row, in result]
    ret = {
        'result': result
    }
    return ret