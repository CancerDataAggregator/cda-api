class CDABaseException(Exception):
    def __init__(self, message: str):
        self.message = message
        self.name = self.__class__.__name__
        self.status_code = None

class ClientErrorException(CDABaseException):
    def __init__(self, message: str):
        super().__init__(message)
        self.status_code = 400
    
class InternalErrorException(CDABaseException):
    def __init__(self, message: str):
        super().__init__(message)
        self.status_code = 500

class ColumnNotFound(ClientErrorException):
    """ Custom exception for when a referenced column is not found"""
    pass


class TableNotFound(ClientErrorException):
    """Custom exception for when a referenced table is not found"""
    pass


class RelationshipError(InternalErrorException):
    """Custom exception for when there is an issue mapping out entity table relationships"""
    pass

class MappingError(InternalErrorException):
    """Custom exception for when there is no mapping found between two tables"""
    pass

class RelationshipNotFound(InternalErrorException):
    """Custom exception for when there is no relationship found between two tables"""
    pass


class SystemNotFound(ClientErrorException):
    """Custom exception for when there is no data system column found"""
    pass


class ParsingError(ClientErrorException):
    """Custom exception for when there is an issue with parsing a filter"""
    pass


class EmptyQueryError(ClientErrorException):
    """Custom exception for when the RequestBody is empty"""
    pass

class DatabaseConnectionDrop(InternalErrorException):
    """Error raised when there is a drop in the API's connection to the database"""
    pass

class InvalidFilterError(ClientErrorException):
    """Custom exception for when the RequestBody is empty"""
    pass