class CDABaseException(Exception):
    def __init__(self, message: str):
        self.message = message
        self.name = self.__class__.__name__
        self.status_code = None

class ClientErrorExcpetion(CDABaseException):
    def __init__(self, message: str):
        super().__init__(message)
        self.status_code = 400
    
class InternalErrorExcpetion(CDABaseException):
    def __init__(self, message: str):
        super().__init__(message)
        self.status_code = 500

class ColumnNotFound(ClientErrorExcpetion):
    """ Custom exception for when a referenced column is not found"""
    pass


class TableNotFound(ClientErrorExcpetion):
    """Custom exception for when a referenced table is not found"""
    pass


class RelationshipError(InternalErrorExcpetion):
    """Custom exception for when there is an issue mapping out entity table relationships"""
    pass

class MappingError(InternalErrorExcpetion):
    """Custom exception for when there is no mapping found between two tables"""
    pass

class RelationshipNotFound(InternalErrorExcpetion):
    """Custom exception for when there is no relationship found between two tables"""
    pass


class SystemNotFound(ClientErrorExcpetion):
    """Custom exception for when there is no data system column found"""
    pass


class ParsingError(ClientErrorExcpetion):
    """Custom exception for when there is an issue with parsing a filter"""
    pass


class EmptyQueryError(ClientErrorExcpetion):
    """Custom exception for when the QNode is empty"""
    pass

class DatabaseConnectionDrop(InternalErrorExcpetion):
    """Error raised when there is a drop in the API's connection to the database"""
    pass
