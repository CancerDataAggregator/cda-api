from pydantic import BaseModel, Field
from typing import Optional, Any


class QNode(BaseModel):
    MATCH_ALL: list[str] | None = None
    MATCH_SOME: list[str] | None = None
    ADD_COLUMNS: list[str] | None = None
    EXCLUDE_COLUMNS: list[str] | None = None
    def as_dict(self):
        return {"MATCH_ALL": self.MATCH_ALL, 
                "MATCH_SOME": self.MATCH_SOME,
                "ADD_COLUMNS": self.ADD_COLUMNS,
                "EXCLUDE_COLUMNS": self.EXCLUDE_COLUMNS}
    def as_string(self):
        return str(self.as_dict()).replace("'", '"')

class PagedResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")
    query_sql: str | None = Field(description="SQL Query generated to yield the results")
    total_row_count: int | None = Field(default=None, description="Count of total number of results from the query")
    next_url: Optional[str] = Field(default=None, description="URL to get to next page of results", )

class SummaryResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")
    query_sql: str | None = Field(description="SQL Query generated to yield the results")

# TODO: change to represent actual frequency result
class UniqueValueResponseObj(BaseModel):
    result: list[dict[str, Any] | None] = Field(description="List of query result json objects")
    query_sql: str | None = Field(description="SQL Query generated to yield the results")
    total_row_count: int | None = Field(default=None, description="Count of total number of results from the query")
    next_url: Optional[str] = Field(default=None, description="URL to get to next page of results", )

# TODO: change to represent actual release metadata result
class ReleaseMetadataObj(BaseModel):
    result: list[dict[str, Any] | None]