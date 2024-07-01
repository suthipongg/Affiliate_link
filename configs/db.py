from pymongo import MongoClient
from elasticsearch import Elasticsearch
import urllib.parse
import os

# Load MongoDB connection details from environment variables
mongodb_user = os.getenv('MONGODB_USER')
mongodb_password = os.getenv('MONGODB_PASSWORD')
mongodb_host = os.getenv('MONGODB_HOST')
mongodb_port = int(os.getenv('MONGODB_PORT'))
mongodb_db = os.getenv('MONGODB_DB')

# Create a MongoDB URI with authentication
if mongodb_user and mongodb_password:
    mongodb_uri = f"mongodb://{urllib.parse.quote_plus(mongodb_user)}:{urllib.parse.quote_plus(mongodb_password)}@{mongodb_host}:{mongodb_port}/{mongodb_db}"
else:
    mongodb_uri = f"mongodb://{mongodb_host}:{mongodb_port}/{mongodb_db}"

# Establish a connection to the MongoDB server
db_connection = MongoClient(mongodb_uri)

# Access your MongoDB collections
db = db_connection[mongodb_db]
product_collection = db[os.getenv('COLLECTION_PRODUCT')]
affiliate_link_collection = db[os.getenv('COLLECTION_AFFILIATE_LINK')]

# Define Elasticsearch configuration
es_config = {
    "host": os.getenv('ES_HOST'),
    "port": int(os.getenv('ES_PORT')),  # Convert port to integer
    "api_version": os.getenv('ES_VERSION'),
    "timeout": 60 * 60,
    "use_ssl": False
}

# Check if authentication variables are defined in .env
es_user = os.getenv('ES_USER')
es_password = os.getenv('ES_PASSWORD')

# Add HTTP authentication to the configuration if username and password are provided
if es_user and es_password:
    es_config["http_auth"] = (es_user, es_password)

# Create an Elasticsearch client instance
es_client = Elasticsearch(**es_config)
