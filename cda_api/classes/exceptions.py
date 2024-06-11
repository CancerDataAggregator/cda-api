class MappingError(Exception):
    """Custom exception for when there is no mapping found between two tables"""
    pass

class ColumnNotFound(Exception):
    """Custom exception for when a referenced column is not found"""
    pass

class TableNotFound(Exception):
    """Custom exception for when a referenced table is not found"""
    pass