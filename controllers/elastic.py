from elasticsearch.helpers import bulk
from datetime import datetime
import json, os

from configs.db import es_client
from configs.logger import logger

class Elastic():
    def __init__(self) -> None:
        self.client = es_client
        self.prefix = os.getenv('PREFIX', '')
        self.analysis = self.load_analysis_settings()
        self.settings = {"analysis": self.analysis}
        
    def load_analysis_settings(self):
        # Load settings from "analysis.json" file
        analysis_file_path = os.path.join(os.path.dirname(__file__), '../configs/analysis.json')
        if os.path.exists(analysis_file_path):
            with open(analysis_file_path, "r") as analysis_file:
                return json.load(analysis_file)
        logger.info("Info: 'analysis.json' file not found.")
        return {}

    def map_sql_type_to_es_type(self, sql_type: str) -> dict:
        sql_type = sql_type.lower()
        if "varchar" in sql_type or "text" in sql_type or "string" in sql_type :
            return {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    },
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete",
                        "search_analyzer": "standard",
                    }
                }
            }
        elif "int" in sql_type or 'decimal' in sql_type or 'integer' in sql_type or 'long' in sql_type:
            return {
                "type": "long",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            }
        elif "date" in sql_type or "timestamp" in sql_type:
            return {
                    "type": "date",
                    }
        elif "array" in sql_type:
            return {
                    "type": "object",
                    "enabled": False
                    }
        elif "nested" in sql_type or "dict" in sql_type:
            return {
                    "type": "nested",
                    }
        else:
            return {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    },
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete",
                        "search_analyzer": "standard",
                    }
                }
            }
            
    def create_mappings(self, columns_data: dict) -> dict:
        properties = {}
        for column_name, data_type in columns_data.items():
            properties[column_name] = self.map_sql_type_to_es_type(data_type['type'])
            if data_type['type'] == 'nested':
                properties[column_name]['properties'] = {}
                for key, value in columns_data[column_name]['properties'].items():
                    properties[column_name]['properties'][key] = self.map_sql_type_to_es_type(value['type'])

        return {
                "properties": properties
            }

    def create_index_es(self, model, mappings=None):
        if isinstance(model, str):
            if mappings is None:
                raise ValueError('mappings is required when model is string')
            alias_name = model
        else:
            alias_name = 'cosme' + '_' + model.__name__.lower()
        
        if not self.client.indices.exists_alias(index='*', name=alias_name):
            if mappings is None or not isinstance(model, str):
                mapping_dict = {}
                for key, value in model.schema()['properties'].items():
                    if key in ['created_at', 'updated_at']:
                        mapping_dict[key] = {'type': 'timestamp'}
                    else:
                        mapping_dict[key] = value
                # create mapping of es
                mappings = self.create_mappings(columns_data=mapping_dict)
                
            index_name = alias_name + '_' + datetime.now().strftime("%Y%m%d.%H%M")
            # first create index and alias
            created_indices = self.client.indices.create(index=index_name, settings=self.settings, mappings=mappings, ignore=[400, 404])
            logger.info(created_indices)
            # create alias
            set_alias = self.client.indices.put_alias(index=index_name, name=alias_name)
            logger.info(set_alias)
            return True
        return False

    def migrate_data(self, index_name: str, datas: list, ids: list=None):
        if ids is not None and len(datas) != len(ids):
            raise ValueError('Length of datas and ids must be equal')
        elif ids is None:
            ids = [None] * len(datas)
        
        actions = [
            {
                "_index": index_name,
                "_op_type": "index",  # Use "index" for indexing documents
                "_id": id if id is not None else data['id'],
                "_source": data  # Your document data
            }
            for data, id in zip(datas, ids)
        ]

        success, failed = bulk(
            self.client,
            actions,
        )
        logger.info(f"Indexed {success} documents successfully. Failed to index {failed} documents.")
        return success, failed
    
    def es_update(self, index_name: str, data_dict: dict, id=None):
        updated = self.client.update(index=index_name, id=id if id is not None else data_dict['id'], body={'doc': data_dict})
        logger.info(updated)
        return updated
    
    def es_update_by_query(self, index_name: str, query: dict={"match_all": {}}, data_dict: dict={}):
        _inline = ''
        data_dict_display = {}
        for key in data_dict.keys():
            _inline += f"ctx._source.{key}=params.{key}; "
            if type(data_dict[key]) == str:
                data_dict_display[key] = data_dict[key][:50]
            elif type(data_dict[key]) == list:
                data_dict_display[key] = data_dict[key][:5]
            else:
                data_dict_display[key] = data_dict[key]
        q = {
            "query": query,
            "script": {
                "source": _inline,
                "params": data_dict,
                "lang": "painless"
            }
        }
        q_display = q.copy()
        q_display['script']['params'] = data_dict_display
        logger.info(q_display)
        updated = self.client.update_by_query(index=index_name, body=q)
        return updated
    
    def es_delete_by_query(self, index_name: str, query: dict):
        datas = self.client.search(index=index_name, query=query)['hits']['hits']
        deleted = self.client.delete_by_query(index=index_name, body={"query": query})
        logger.info(deleted)
        return datas, deleted['deleted']