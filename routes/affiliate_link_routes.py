from fastapi import APIRouter, HTTPException, status, Depends, Query
from bson import ObjectId
import os, json
from dotenv import load_dotenv
load_dotenv()

from configs.security import UnauthorizedMessage, get_token
from configs.db import (
    affiliate_link_collection
)
from configs.logger import logger
from controllers.elastic import Elastic
from models.affiliate_link_model import AffiliateLinkModel
from models.find_model import FindModel
from schemas.affiliate_link_schema import affiliate_links_serializer
from utils.exception_handling import handle_exceptions
from utils.affiliate_link_checking import status_affiliate_link
from controllers.utils_elastic import (
    parse_sort_criteria,
    parse_filter_criteria,
)

elastic = Elastic()

affiliate_link_route = APIRouter(tags=["AFFILIATE LINK"])

@affiliate_link_route.post(
    "/affiliate_link/find",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)}
)
@handle_exceptions
async def find_all_affiliate_link(
    find: FindModel,
    token_auth: str = Depends(get_token),
):
    find = find.model_dump()
    if find['page_start'] < 1:
        raise HTTPException(
            status_code=400,
            detail="Page must be greater than or equal to 1",
        )

    query = {}
    if find['filter'].strip():
        filter_body = json.loads(find['filter'].strip())
        if filter_body:
            query["query"] = parse_filter_criteria(filter_body)
    total_hits = elastic.client.count(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body=query)['count']
    page_count = (total_hits - 1)//find['page_size'] + 1
    pagination = {
        "page": find['page_start'],
        "pageSize": find['page_size'],
        "pageCount": page_count,
        "total": total_hits
    }
    query["size"] = find['page_size']
    query["track_total_hits"] = True
    if find['exclude_fields']:
        query["_source"] = {"excludes": [exc.strip() for exc in find['exclude_fields'].split(",")]}
        
    is_reverse_sort = (find['page_start'] == page_count or find['page_start'] < find['previous_page']) and find['page_start'] != 1
    after_value = find['first_value'] if is_reverse_sort else find['last_value']
    query["sort"] = parse_sort_criteria(find['sort_by']+",id.keyword" if find['sort_by'] else "id.keyword", is_reverse_sort)
    if find['page_start'] > 1 and find['page_start'] != page_count:
        if find['page_start'] != find['previous_page']:
            for i in range(abs(find['page_start'] - find['previous_page'])):
                query['search_after'] = after_value
                result = elastic.client.search(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body=query)['hits']['hits']
                after_value = result[-1]['sort']
        elif find['page_start'] == find['previous_page']:
            query['search_after'] = after_value
            result = elastic.client.search(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body=query)['hits']['hits']
    else:
        result = elastic.client.search(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body=query)['hits']['hits']
    result = result if not is_reverse_sort else result[::-1]
    result = result[-(total_hits - find['page_size']*(page_count-1)):] if find['page_start'] == page_count else result
    
    first_value, last_value = result[0]['sort'], result[-1]['sort']
    # Serialize the results
    extracted_list = affiliate_links_serializer(result)
    # Calculate pagination metadata
    return {"detail": "Success", "data": extracted_list, "meta": {"pagination": pagination}, "params": {"first_value": first_value, "last_value": last_value}}


@affiliate_link_route.post(
    "/affiliate_link/insert",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_201_CREATED,
)
@handle_exceptions
async def insert_affiliate_link(
    body: AffiliateLinkModel,
    token_auth: str = Depends(get_token)
):
    body = body.model_dump()
    if elastic.client.search(index=os.getenv('COLLECTION_PRODUCT'), body={"query": {"term": {"product_id": body['product_id']}}})['hits']['total']['value'] == 0:
        raise HTTPException(status_code=400, detail="Product not exists.")
    # elif elastic.client.search(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body={"query": {"term": {"link.keyword": body['link']}}})['hits']['total']['value'] > 0:
    #     raise HTTPException(status_code=400, detail="Link already exists.")
    
    body['active'], description = status_affiliate_link(body['link'])
    body['description'] = body['description'] + ': ' + description
    res_mg = affiliate_link_collection.insert_one(body)
    if not res_mg.acknowledged:
        raise HTTPException(status_code=400, detail="Insert to mongodb failed.")
    body['id'] = str(body.pop('_id'))
    res_es = elastic.client.index(index=os.getenv('COLLECTION_AFFILIATE_LINK'), id=body['id'], body=body, refresh=True)
    if res_es['result'] != 'created':
        raise HTTPException(status_code=400, detail="Insert to elasticsearch failed.")
    return {"detail": "Insert successful.", "data": body}


@affiliate_link_route.put(
    "/affiliate_link/update/{id}",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def update_affiliate_link(
    id: str, 
    body: AffiliateLinkModel,
    token_auth: str = Depends(get_token)
):
    body = body.model_dump()
    old_data = elastic.client.get(index=os.getenv('COLLECTION_AFFILIATE_LINK'), id=id)['_source']
    if not old_data:
        raise HTTPException(status_code=400, detail="Data not found.")
    body['active'], description = status_affiliate_link(body['link'])
    body['description'] = body['description'] + ': ' + description
    res_mg = affiliate_link_collection.update_one({"_id": ObjectId(id)}, {"$set": body})
    if not res_mg.acknowledged:
        raise HTTPException(status_code=400, detail="Update to mongodb failed.")
    body['id'] = str(id)
    res_es = elastic.client.update(index=os.getenv('COLLECTION_AFFILIATE_LINK'), id=id, body={"doc": body}, refresh=True)
    if res_es['_shards']['failed'] != 0:
        raise HTTPException(status_code=400, detail="Update to elasticsearch failed.")
    return {"detail": "Update successful.", "data": body}


@affiliate_link_route.delete(
    "/affiliate_link/delete/{id}",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def delete_affiliate_link(
    id: str, 
    token_auth: str = Depends(get_token)
):
    if not elastic.client.exists(index=os.getenv('COLLECTION_AFFILIATE_LINK'), id=id):
        raise HTTPException(status_code=400, detail="Data not found.")
    
    data = affiliate_link_collection.find_one_and_delete({"_id": ObjectId(id)})
    if not data:
        raise HTTPException(status_code=400, detail="Delete from mongodb failed.")
    res_es = elastic.client.delete(index=os.getenv('COLLECTION_AFFILIATE_LINK'), id=id, refresh=True)
    if res_es['result'] != 'deleted':
        raise HTTPException(status_code=400, detail="Delete from elasticsearch failed.")
    data['id'] = str(data.pop('_id'))
    return {"detail": "Delete successful.", "data": data}