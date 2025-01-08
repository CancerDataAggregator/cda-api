from sqlalchemy import func

from cda_api.db.query_utilities import query_to_string
from cda_api.db.schema import Base


def get_release_metadata(db, log):
    log.info("Building release_metadata query")
    subquery = db.query(Base.metadata.tables["release_metadata"]).subquery("subquery")
    query = db.query(func.row_to_json(subquery.table_valued()))
    log.debug(f'Query:\n{"-"*60}\n{query_to_string(query)}\n{"-"*60}')

    result = query.all()
    result = [row for (row,) in result]
    ret = {"result": result}
    return ret
