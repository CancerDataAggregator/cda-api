from sqlalchemy import func

class ReleaseMetadataQuery:
    def __init__(self, db, db_info):
        self.db = db
        self.db_info = db_info
        self.release_metadata_table_info = self.db_info.get_table_info('release_metadata')
    
    def get_query(self):
        subquery = self.db.query(self.release_metadata_table_info.db_table).subquery("subquery")
        query = self.db.query(func.row_to_json(subquery.table_valued()))
        return query

