class MappingError(Exception):
    """Custom exception for when there is no mapping found between two tables"""
    pass

class ColumnNotFound(Exception):
    """Custom exception for when a referenced column is not found"""
    pass

class TableNotFound(Exception):
    """Custom exception for when a referenced table is not found"""
    pass

class RelationshipError(Exception):
    """Custom exception for when there is an issue mapping out entity table relationships"""
    pass

class RelationshipNotFound(Exception):
    """Custom exception for when there is no relationship found between two tables"""
    pass

class SystemNotFound(Exception):
    """Custom exception for when there is no data system column found"""
    pass

class ParsingError(Exception):
    """Custom exception for when there is an issue with parsing a filter"""
    pass