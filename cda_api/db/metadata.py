from cda_api.db.schema import Base
from cda_api.db.query_utilities import query_to_string
from cda_api import get_logger

log = get_logger()

def get_release_metadata(db):
    query = db.query(Base.metadata.tables['release_metadata'])
    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')
    # Fake return for now
    ret = {
        'result': [{'release_metadata': 'success'}]
    }
    return ret