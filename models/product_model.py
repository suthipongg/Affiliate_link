from pydantic import BaseModel
from datetime import datetime

class ProductModel(BaseModel):
    product_id: int
    product_name: str
    modify_date: datetime = None
    active: bool
    description: str = ''

    def __init__(self, **data):
        super().__init__(**data)
        self.modify_date = datetime.now()

    class Config:
        json_schema_extra = {
            "example": {
                "product_id": 0,
                "product_name": "sk-ii",
                "modify_date": "2021-10-01 00:00:00",
                "active": True,
            }
        }
