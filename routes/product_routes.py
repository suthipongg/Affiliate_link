from fastapi import APIRouter, HTTPException, status, Depends
from bson import ObjectId
import os
from dotenv import load_dotenv
load_dotenv()

from configs.security import UnauthorizedMessage, get_token
from configs.db import (
    product_collection,
    affiliate_link_collection
)
from configs.logger import logger
from controllers.elastic import Elastic
from models.product_model import ProductModel
from schemas.affiliate_link_schema import affiliate_links_serializer
from schemas.product_schema import products_serializer
from utils.exception_handling import handle_exceptions

elastic = Elastic()

product_route = APIRouter(tags=["PRODUCT"])

@product_route.get(
    "/product/find",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)}
)
@handle_exceptions
async def find_all_product(
    token_auth: str = Depends(get_token)
):
    datas = elastic.client.search(index=os.getenv('COLLECTION_PRODUCT'), body={"query": {"match_all": {}}})['hits']['hits']
    datas = products_serializer(datas)
    return {"detail": "Success", "data": datas}


@product_route.post(
    "/product/insert",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_201_CREATED,
)
@handle_exceptions
async def insert_product(
    body: ProductModel,
    token_auth: str = Depends(get_token)
):
    body = body.model_dump()
    if elastic.client.search(index=os.getenv('COLLECTION_PRODUCT'), body={"query": {"term": {"product_id": body['product_id']}}})['hits']['total']['value'] > 0:
        raise HTTPException(status_code=400, detail="Product ID already exists.")
    
    res_mg = product_collection.insert_one(body)
    if not res_mg.acknowledged:
        raise HTTPException(status_code=400, detail="Insert to mongodb failed.")
    body['id'] = str(body.pop('_id'))
    res_es = elastic.client.index(index=os.getenv('COLLECTION_PRODUCT'), id=body['id'], body=body, refresh=True)
    if res_es['result'] != 'created':
        raise HTTPException(status_code=400, detail="Insert to elasticsearch failed.")
    return {"detail": "Insert successful.", "data": body}
    

@product_route.put(
    "/product/update/{id}",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def update_product(
    id: str, 
    body: ProductModel,
    token_auth: str = Depends(get_token)
):
    body = body.model_dump()
    old_data = elastic.client.get(index=os.getenv('COLLECTION_PRODUCT'), id=id)['_source']
    if not old_data:
        raise HTTPException(status_code=400, detail="Data not found.")
    
    res_mg = product_collection.update_one({"_id": ObjectId(id)}, {"$set": body})
    if not res_mg.acknowledged:
        raise HTTPException(status_code=400, detail="Update to mongodb failed.")
    body['id'] = id
    res_es = elastic.client.update(index=os.getenv('COLLECTION_PRODUCT'), id=id, body={"doc": body}, refresh=True)
    if res_es['_shards']['failed'] != 0:
        raise HTTPException(status_code=400, detail="Update to elasticsearch failed.")
    return {"detail": "Update successful.", "data": body}
    
@product_route.delete(
    "/product/delete/{id}",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def delete_product(
    id: str, 
    token_auth: str = Depends(get_token)
):
    if not elastic.client.exists(index=os.getenv('COLLECTION_PRODUCT'), id=id):
        raise HTTPException(status_code=400, detail="Data not found.")
    
    data = elastic.client.get(index=os.getenv('COLLECTION_PRODUCT'), id=id)['_source']
    aff_data = elastic.client.search(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body={"query": {"term": {"product_id": data['product_id']}}})['hits']['hits']
    affiliate_link_collection.delete_many({"product_id": data['product_id']})
    elastic.client.delete_by_query(index=os.getenv('COLLECTION_AFFILIATE_LINK'), body={"query": {"term": {"product_id": data['product_id']}}}, refresh=True)
    data['affiliate_links'] = affiliate_links_serializer(aff_data)
    
    res_mg = product_collection.delete_one({"_id": ObjectId(id)})
    if not res_mg.acknowledged:
        raise HTTPException(status_code=400, detail="Delete from mongodb failed.")
    
    res_es = elastic.client.delete(index=os.getenv('COLLECTION_PRODUCT'), id=id, refresh=True)
    if res_es['result'] != 'deleted':
        raise HTTPException(status_code=400, detail="Delete from elasticsearch failed.")
    return {"detail": "Delete successful.", "data": data}