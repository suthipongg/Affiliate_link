from controllers.elastic import Elastic
import os

elastic = Elastic()

product_mappings = {
    "properties": {
        "product_id": {
            "type": "long",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "product_name": {
            "type": "text",
        },
        "modify_date": {
            "type": "date"
        },
        "check_date": {
            "type": "date"
        },
        "active": {
            "type": "boolean"
        }
    }
}

affiliate_link_mapping = {
    "properties": {
        "product_id": {
            "type": "long",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "shop": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "link": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "active": {
            "type": "boolean"
        },
        "description": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "check_date": {
            "type": "date"
        },
    }
}

def create_index_es():
    elastic.create_index_es(os.getenv('COLLECTION_PRODUCT'), mappings=product_mappings)
    elastic.create_index_es(os.getenv('COLLECTION_AFFILIATE_LINK'), mappings=affiliate_link_mapping)