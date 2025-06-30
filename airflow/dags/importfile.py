import pandas as pd
from pymongo import MongoClient
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, InvalidURI, ConfigurationError
from contextlib import contextmanager
import os
from dotenv import load_dotenv
import urllib 
load_dotenv(dotenv_path="./.env")

df = pd.read_csv("music_feature.csv")

def auth_mongoDB():
    user =  urllib.parse.quote_plus(os.getenv("MONGODB_USER", ""))
    password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD", ""))
    cluster = os.getenv("MONGODB_SRV", "").replace("mongodb+srv://", "").strip("/")
    uri = f"mongodb+srv://{user}:{password}@{cluster}/?retryWrites=true&w=majority"
    print("MongoDB URI:", uri)
    return uri

uri = auth_mongoDB()

client = MongoClient(uri)
db = client["spotify"]
db.drop_collection("feature_music")
print("Đã xóa collection feature_music.")


records = df.to_dict(orient="records")
db["feature_music"].insert_many(records)
print(f"Đã import {len(records)} bản ghi vào collection feature_music.")