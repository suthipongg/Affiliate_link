from pydantic import BaseModel
from datetime import datetime

class AffiliateLinkModel(BaseModel):
    product_id: int
    shop: str
    link: str
    active: bool = None
    description: str = ''
    check_date: datetime = None

    def __init__(self, **data):
        super().__init__(**data)
        self.check_date = datetime.now()

    class Config:
        json_schema_extra = {
            "example": {
                "product_id": 0,
                "shop": "lazada",
                "link": "https://s.lazada.co.th/s.jmwgO",
            }
        }
