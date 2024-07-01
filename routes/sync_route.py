from fastapi import APIRouter, status, Depends
import pymysql.cursors
from bs4 import BeautifulSoup

from configs.security import UnauthorizedMessage, get_token
from configs.logger import logger
from configs.db import (
    affiliate_link_collection,
)
from controllers.elastic import Elastic
from models.affiliate_link_model import AffiliateLinkModel
from models.product_model import ProductModel
from routes.affiliate_link_routes import insert_affiliate_link, update_affiliate_link, delete_affiliate_link
from routes.product_routes import insert_product, update_product, delete_product
from utils.exception_handling import handle_exceptions
import pymysql, os, json
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv, dotenv_values, set_key
load_dotenv()

affiliate_sync_route = APIRouter(tags=["SYNC"])
elastic = Elastic()

# Connect to MySQL
def connect_mysql():
    mysql_conn = pymysql.connect(host=os.getenv('MYSQL_HOST', ''),
                                user=os.getenv('MYSQL_USER', ''),
                                password=os.getenv('MYSQL_PASSWORD', ''),
                                database=os.getenv('MYSQL_DB', ''),
                                port=int(os.getenv('MYSQL_PORT', '')))
    mysql_cursor = mysql_conn.cursor()
    return mysql_cursor, mysql_conn

query_add = lambda date: f"""
select 
	product.ID
	, product.NAME
	, product.ACTIVE
	, product.DETAIL_TEXT
	, product.TIMESTAMP_X
	, aff.lazada
from 
	b_iblock_element as product
left join (
	select 
		pd.ID
		, max(CASE WHEN p_elem_prop.IBLOCK_PROPERTY_ID = 260 THEN p_elem_prop.VALUE end) as lazada
	from 
		b_iblock_element_property as p_elem_prop
	left join b_iblock_element as pd ON pd.ID = p_elem_prop.IBLOCK_ELEMENT_ID 
	WHere 
		(p_elem_prop.IBLOCK_PROPERTY_ID in (260))
		and (pd.IBLOCK_ID = 17)
	group by pd.ID
) as aff ON product.ID = aff.ID
WHere 
    (product.IBLOCK_ID = 17)
    and product.TIMESTAMP_X >= '{date}'
order by product.TIMESTAMP_X
"""

query_remove_product = """
SELECT 
	product.ID
FROM b_iblock_element product
WHERE (product.IBLOCK_ID = 17)
"""

query_remove_link = """
SELECT
    aff.ID
FROM b_iblock_element_property as p_elem_prop
left join b_iblock_element as aff ON aff.ID = p_elem_prop.IBLOCK_ELEMENT_ID
WHERE (p_elem_prop.IBLOCK_PROPERTY_ID in (260, 265))
    and (aff.IBLOCK_ID = 17)
"""

def get_latest_date():
    query = {
        "size": 0,
        "aggs": {
            "most_recent": {
                "max": {"field": "modify_date"}
            }
        }
    }
    last_time = elastic.client.search(index=os.getenv('COLLECTION_PRODUCT', ''), body=query)['aggregations']['most_recent']
    last_time = last_time['value_as_string'] if last_time['value'] else '1970-01-01T00:00:00.000Z'
    return parser.isoparse(last_time).strftime('%Y-%m-%d %H:%M:%S')

def preprocess_product_data(data):
    body = {
        "product_id": 0,
        "product_name": "sk-ii",
        "modify_date": "2021-10-01 00:00:00",
        "active": True,
    }
    body['product_id'] = int(data['ID'])
    body['product_name'] = data['NAME']
    body['modify_date'] = data['TIMESTAMP_X']
    body['active'] = data['ACTIVE'] == 'Y'
    return ProductModel(**body)

def preprocess_affiliate_data(data, shop, description=''):
    body = {
        "product_id": 0,
        "shop": "lazada",
        "link": "https://s.lazada.co.th/s.jmwgO",
        'description': ''
    }
    body['product_id'] = int(data['ID'])
    body['shop'] = shop
    body['link'] = data[shop]
    body['description'] = description
    return AffiliateLinkModel(**body)

async def insert_data(data, shop, description='', token_auth=None):
    body = preprocess_affiliate_data(data, shop, description)
    response = await insert_affiliate_link(body, token_auth)
    return response, body

def find_link(html):
    shop = {}
    try:
        soup = BeautifulSoup(html, 'lxml')
    except:
        logger.info(html)
        raise Exception("Error")
    a_tags = soup.find_all('a')
    for tag in a_tags:
        img_tag = tag.find('img')
        if img_tag and 'src' in img_tag.attrs:
            src = img_tag['src']
            if 'lazada' in src:
                shop['lazada'] = tag['href']
    return shop

# check if data in mysql is newer than data in mongo
async def check_insert_and_update_data(info, mysql_cursor, token_auth):
    latest_time = get_latest_date()
    mysql_cursor.execute(query_add(latest_time))
    result = mysql_cursor.fetchall()
    column_name = list(map(lambda x: x[0], mysql_cursor.description))
    logger.info(f"Total: {len(result)}")
    for i, row in enumerate(result):
        if i % 100 == 0:
            logger.info(f"Progress: {i+1}/{len(result)}")
        data = dict(zip(column_name, row))
        product = preprocess_product_data(data)
        q = {"query": {"term": {"product_id": int(data['ID'])}}}
        data_es = elastic.client.search(index=os.getenv('COLLECTION_PRODUCT', ''), body=q)['hits']['hits']
        if not data_es:
            response = await insert_product(product, token_auth)
            if response['detail'] == 'Insert successful.':
                info['product_inserted'].append(response['data'])
            else:
                product = product.model_dump()
                product['function'] = 'product_inserted'
                info['not_sync'].append(product)
        else:
            id = data_es[0]['_id']
            response = await delete_product(id, token_auth)
            response = await insert_product(product, token_auth)
            if response['detail'] == 'Insert successful.':
                info['product_updated'].append(response['data'])
            else:
                product = product.model_dump()
                product['function'] = 'product_updated'
                info['not_sync'].append(product)
        if isinstance(data['lazada'], str):
            response, body = await insert_data(data, 'lazada', 'New method', token_auth)
            if response['detail'] == 'Insert successful.':
                info['link_checked'].append(response['data'])
            else:
                body = body.model_dump()
                body['function'] = 'link_checked'
                info['not_sync'].append(body)
        old_shop = find_link(data['DETAIL_TEXT'])
        if isinstance(old_shop.get('lazada', None), str):
            data['lazada'] = old_shop.get('lazada')
            response, body = await insert_data(data, 'lazada', 'Old method', token_auth)
            if response['detail'] == 'Insert successful.':
                info['link_checked'].append(response['data'])
            else:
                body = body.model_dump()
                body['function'] = 'link_checked'
                info['not_sync'].append(body)
    if i % 100 != 0:
        logger.info(f"Progress: {i+1}/{len(result)}")

# check if amount of data in mysql not equal to the mongo
async def check_delete_data(info, mysql_cursor, token_auth):
    q_sql = "SELECT COUNT(*) FROM b_iblock_element WHERE (IBLOCK_ID = 17)"
    mysql_cursor.execute(q_sql)
    # if mysql_cursor.fetchall()[0][0] == elastic.client.count(index=os.getenv('COLLECTION_PRODUCT', ''))['count']:
    #     return
    mysql_cursor.execute(query_remove_product)
    result = mysql_cursor.fetchall()
    q = {"query": {"bool": {"must_not": {"terms": {"product_id": [int(row[0]) for row in result]}}}}}
    data_es = elastic.client.search(index=os.getenv('COLLECTION_PRODUCT', ''), body=q)['hits']['hits']
    for data in data_es:
        response = await delete_product(data['_id'], token_auth)
        if response['detail'] == 'Delete successful.':
            info['product_deleted'].append(data)
        else:
            data['function'] = 'delete'
            info['not_sync'].append(data)
    mysql_cursor.execute(q_sql)
    if mysql_cursor.fetchall()[0][0] != elastic.client.count(index=os.getenv('COLLECTION_PRODUCT', ''))['count']:
        info['not_sync'].append({"detail": "Data not sync correctly"})


@affiliate_sync_route.get(
    "/affiliate/sync/database",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
# @handle_exceptions
async def sync_database(
    token_auth: str = Depends(get_token)
):
    mysql_cursor, mysql_conn = connect_mysql()
    
    info = {'product_inserted':[], 'product_updated':[], 'product_deleted':[], 'link_checked': [], 'not_sync':[]}
    
    await check_insert_and_update_data(info, mysql_cursor, token_auth)
    await check_delete_data(info, mysql_cursor, token_auth)
    
    mysql_cursor.close()
    mysql_conn.close()
    return {"detail": "Success", "info": info}


@affiliate_sync_route.get(
    "/affiliate/amount/check",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
# @handle_exceptions
async def check_amount_all_link(
    start_date: datetime = None,
    end_date: datetime = datetime.now(),
    token_auth: str = Depends(get_token)
):
    await sync_database(token_auth)
    find_query = {'check_date': {'$lte': end_date}}
    if start_date:
        find_query['check_date']['$gte'] = start_date
    all_link = affiliate_link_collection.count_documents(find_query)
    return {"total": all_link}


@affiliate_sync_route.get(
    "/affiliate/check/product/all",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
# @handle_exceptions
async def check_aff_all_link(
    start_date: datetime = None,
    end_date: datetime = datetime.now(),
    token_auth: str = Depends(get_token)
):
    await sync_database(token_auth)
    results = {'success': [], 'fail': []}
    
    find_query = {'check_date': {'$lte': end_date}}
    if start_date:
        find_query['check_date']['$gte'] = start_date
    all_link = affiliate_link_collection.find(find_query)
    all_link = list(all_link)
    logger.info(f"Total: {len(all_link)}")
    for i, link in enumerate(all_link):
        link['check_date'] = datetime.now()
        link['description'] = link['description'].split(':')[0] if len(link['description'].split(':')) > 1 else ''
        id = link.pop('_id')
        link_model = AffiliateLinkModel(**link)
        res = await update_affiliate_link(id, link_model, token_auth)
        if res['detail'] == 'Update successful.':
            results['success'].append(link)
        else:
            results['fail'].append(link)
        logger.info(f"Progress: {i+1}/{len(all_link)}")
    return results


@affiliate_sync_route.get(
    "/affiliate/check/product/{product_id}",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
# @handle_exceptions
async def check_aff_id_prod(
    product_id: int,
    token_auth: str = Depends(get_token)
):
    await sync_database(token_auth)
    results = {'success': [], 'fail': []}
    all_link = affiliate_link_collection.find({'product_id': product_id})
    all_link = list(all_link)
    logger.info(f"Total: {len(all_link)}")
    for i, link in enumerate(all_link):
        link['check_date'] = datetime.now()
        link['description'] = link['description'].split(':')[0] if len(link['description'].split(':')) > 1 else ''
        id = str(link.pop('_id'))
        link_model = AffiliateLinkModel(**link)
        res = await update_affiliate_link(id, link_model, token_auth)
        if res['detail'] == 'Update successful.':
            results['success'].append(res['data'])
        else:
            results['fail'].append(res['data'])
        logger.info(f"Progress: {i+1}/{len(all_link)}")
    return results


@affiliate_sync_route.post(
    "/content/sync/environment", 
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def sync_env(
    token_auth: str = Depends(get_token)
):  
    load_dotenv('configs/.env.shared', override=True)
    return {"env": dotenv_values("configs/.env.shared"), "detail": "sync environment success"}


@affiliate_sync_route.get(
    "/content/sync/environment/display", 
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def display_env(
    env_fields: str,
    token_auth: str = Depends(get_token)
):  
    env_info = dotenv_values("configs/.env.shared")
    if env_fields:
        env_info = {env_field.strip():os.getenv(env_field.strip()) for env_field in env_fields.split(",") if env_field.strip() in env_info}
    return {"env": env_info, "detail": "get environment success"}


@affiliate_sync_route.post(
    "/content/sync/environment/set",
    responses={status.HTTP_401_UNAUTHORIZED: dict(model=UnauthorizedMessage)},
    status_code=status.HTTP_200_OK,
)
@handle_exceptions
async def set_env(
    env: str, 
    token_auth: str = Depends(get_token)
):  
    dc_env = json.loads(env)
    key_env = dotenv_values("configs/.env.shared")
    result = {"success": {}, "fail": {}}
    for key, val in dc_env.items():
        try:
            if key in key_env:
                val = str(val)
                result["success"][key] = val
                    
                set_key("configs/.env.shared", key, val)
                os.environ[key] = val
            else:
                result["fail"][key] = "not found"
        except Exception as e:
            result["fail"][key] = str(e)
    return {"env": result, "detail": "set environment success"}