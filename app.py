print('Starting project Affiliate ::: ')
from fastapi import FastAPI, Request
import time, datetime
from configs.middleware import log_request_middleware

from elasticapm import set_context
from starlette.requests import Request
from elasticapm.utils.disttracing import TraceParent
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv
load_dotenv('configs/.env.secret')
load_dotenv('configs/.env.shared')

ALLOWED_ORIGINS = ['*']

app = FastAPI(
    title="Project Affiliate API (2024)", 
    description=f"Created by music \n TNT Media and Network Co., Ltd. \n Started at {datetime.datetime.now().strftime('%c')}",
    docs_url="/",
    version="1.0.0",
    )

app.add_middleware(  
    CORSMiddleware,  
    allow_origins=ALLOWED_ORIGINS,  # Allows CORS for this specific origin  
    allow_credentials=True,  
    allow_methods=["*"],  # Allows all methods  
    allow_headers=["*"],  # Allows all headers  
)  

@app.middleware("http")  
async def add_process_time_header(request: Request, call_next):  
    trace_parent = TraceParent.from_headers(request.headers)  
    set_context({  
        "method": request.method,  
        "url_path": request.url.path,  
    }, 'request')  
    response = await call_next(request)  
    set_context({  
        "status_code": response.status_code,  
    }, 'response')  
    # Check if the status code is not in the range of 200-299  
    if 200 <= response.status_code < 300:  
        transaction_status = 'success'  
    else:  
        transaction_status = 'failure'  
    return response  

app.middleware("http")(log_request_middleware)
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    response.headers["X-Process-Time"] = "{0:.2f}ms".format(process_time)
    return response

from utils.manage_db import create_index_es
create_index_es()

from configs.db import check_redis_connection
check_redis_connection()

from routes.product_routes import product_route
app.include_router(product_route)

from routes.affiliate_link_routes import affiliate_link_route
app.include_router(affiliate_link_route)

from routes.sync_route import affiliate_sync_route
app.include_router(affiliate_sync_route)

print('Started project Affiliate ::: ')