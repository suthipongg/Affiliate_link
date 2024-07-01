from pydantic import BaseModel
from fastapi import Query

class FindModel(BaseModel):
    page_start: int = Query(default=1, description="Starting page number")
    page_size: int = Query(default=10, description="Number of items per page")
    exclude_fields: str = Query(
        default="",
        description="Comma-separated list of fields to exclude from the output",
    )
    sort_by: str = Query(
        default="",
        description="Comma-separated list of fields to sort by (e.g., 'field1,-field2' for descending)",
    )
    filter: str = Query(
        default="",
        description="Filter criteria (e.g., 'field1:value1,field2:*value2*')",
    )
    first_value: list = Query(default=[], description="After value")
    last_value: list = Query(default=[], description="Last value")
    previous_page: int = Query(default=0, description="Previous page")

    def __init__(self, **data):
        super().__init__(**data)

    class Config:
        json_schema_extra = {
            "example": {
                "page_start": 1,
                "page_size": 5,
                "exclude_fields": "",
                "sort_by": "",
                "filter": "",
                "first_value": [],
                "last_value": [],
                "previous_page": 0
            }
        }
