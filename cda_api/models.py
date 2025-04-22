from typing import Any, Optional

from pydantic import BaseModel, Field


class DataRequestBody(BaseModel):
    MATCH_ALL: list[str] | None = []
    MATCH_SOME: list[str] | None = []
    ADD_COLUMNS: list[str] | None = []
    EXCLUDE_COLUMNS: list[str] | None = []
    EXPAND_RESULTS: bool | None = False

    def to_dict(self):
        return {
            "MATCH_ALL": self.MATCH_ALL,
            "MATCH_SOME": self.MATCH_SOME,
            "ADD_COLUMNS": self.ADD_COLUMNS,
            "EXCLUDE_COLUMNS": self.EXCLUDE_COLUMNS,
            "EXPAND_RESULTS": self.EXPAND_RESULTS
        }

    def as_string(self):
        return str(self.to_dict()).replace("'", '"')

    def is_empty(self):
        print("CHECKING")
        if (self.MATCH_ALL is None) and (self.MATCH_SOME is None):
            print("TRUE")
            return True
        else:
            print("FALSE")
            return False

    def replace(self, attribute: str, values: list):
        if attribute not in ["MATCH_ALL", "MATCH_SOME", "ADD_COLUMNS", "EXCLUDE_COLUMNS", "EXPAND_RESULTS"]:
            raise ValueError(f"{attribute} not in list: ['MATCH_ALL', 'MATCH_SOME', 'ADD_COLUMNS', 'EXCLUDE_COLUMNS', 'EXPAND_RESULTS']")
        setattr(self, attribute, values)

    def __eq__(self, value: object) -> bool:
        return super().__eq__(value)
    
    
class SummaryRequestBody(BaseModel):
    MATCH_ALL: list[str] | None = []
    MATCH_SOME: list[str] | None = []
    ADD_COLUMNS: list[str] | None = []
    EXCLUDE_COLUMNS: list[str] | None = []

    def to_dict(self):
        return {
            "MATCH_ALL": self.MATCH_ALL,
            "MATCH_SOME": self.MATCH_SOME,
            "ADD_COLUMNS": self.ADD_COLUMNS,
            "EXCLUDE_COLUMNS": self.EXCLUDE_COLUMNS
        }

    def as_string(self):
        return str(self.to_dict()).replace("'", '"')

    def is_empty(self):
        print("CHECKING")
        if (self.MATCH_ALL is None) and (self.MATCH_SOME is None):
            print("TRUE")
            return True
        else:
            print("FALSE")
            return False

    def replace(self, attribute: str, values: list):
        if attribute not in ["MATCH_ALL", "MATCH_SOME", "ADD_COLUMNS", "EXCLUDE_COLUMNS"]:
            raise ValueError(f"{attribute} not in list: ['MATCH_ALL', 'MATCH_SOME', 'ADD_COLUMNS', 'EXCLUDE_COLUMNS']")
        setattr(self, attribute, values)

    def __eq__(self, value: object) -> bool:
        return super().__eq__(value)

class PagedResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")
    query_sql: str | None = Field(description="SQL Query generated to yield the results")
    total_row_count: int | None = Field(default=None, description="Count of total number of results from the query")
    next_url: Optional[str] = Field(
        default=None,
        description="URL to get to next page of results",
    )


class SummaryResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")
    query_sql: str | None = Field(description="SQL Query generated to yield the results")


class ColumnResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")


# TODO: change to represent actual frequency result
class ColumnValuesResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")
    query_sql: str | None = Field(description="SQL Query generated to yield the results")
    total_row_count: int | None = Field(default=None, description="Count of total number of results from the query")
    next_url: Optional[str] = Field(
        default=None,
        description="URL to get to next page of results",
    )


# TODO: change to represent actual release metadata result
class ReleaseMetadataObj(BaseModel):
    result: list[dict[str, Any] | None]


class InternalError(BaseModel):
    error_type: str
    message: str

class ClientError(BaseModel):
    error_type: str
    message: str