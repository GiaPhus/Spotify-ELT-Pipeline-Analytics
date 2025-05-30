from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, InvalidURI, ConfigurationError
from contextlib import contextmanager
import os
from dotenv import load_dotenv
import urllib 
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../.env'))
load_dotenv(dotenv_path)
mongo_uri = os.getenv("MONGODB_USER")
print(mongo_uri)
@contextmanager
def MongoIO():
    user =  urllib.parse.quote_plus(os.getenv("MONGODB_USER", ""))
    password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD", ""))
    cluster = os.getenv("MONGODB_SRV", "").replace("mongodb+srv://", "").strip("/")
    database = os.getenv("MONGODB_DATABASE", "test")


    if not user:
        raise ValueError(f"Thiếu thông tin kết nối MongoDB ({user})")
        
    if not password:
        raise ValueError("Thiếu thông tin kết nối MongoDB (password)")
    if not database:
        raise ValueError("Thiếu thông tin kết nối MongoDB (database)")
    if not cluster:
        raise ValueError("Thiếu thông tin kết nối MongoDB (cluster)")
    


    uri = f"mongodb+srv://{user}:{password}@{cluster}/{database}?retryWrites=true&w=majority"

    client = None
    try:
        client = MongoClient(uri,serverSelectionTimeoutMS=5000,  # 5 seconds timeout
    connectTimeoutMS=10000)
        print("Kết nối MongoDB thành công")
        yield client

    except (ConnectionFailure, InvalidURI, ConfigurationError) as e:
        print(f"Kết nối MongoDB thất bại: {e}")
        raise

    finally:
        if client is not None:
            print("Đóng kết nối MongoDB")
            client.close()