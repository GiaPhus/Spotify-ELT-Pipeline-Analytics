import csv
import os
import sys
import time
sys.path.append(os.path.abspath("/home/phu/Pictures/Spotify-ELT-Pipeline-Analytics/airflow/plugins"))

from crawl.MongoDB_Connection import MongoDB
from MongoDBio import MongoIO



def import_csv(csv_path,collection_name):
    db_name = os.getenv("MONGODB_DATABASE")

    with MongoIO() as client : 
        mongo = MongoDB(client)
        if not mongo.check_database_exists(db_name):
            db = mongo.create_database(db_name)
        else:
            db = client[os.getenv("MONGODB_DATABASE")]
        
        if not mongo.check_collection_exists(db_name, collection_name):
            coll = mongo.create_collection(db, collection_name)
        else:
            coll = db[collection_name]
        
        with open(csv_path,newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data = list(reader)
            if data:
                mongo.insert_many(data, db, coll)
                print(f"oke")
            else:
                print("Không có dữ liệu trong CSV")
                


import_csv("/home/phu/Pictures/Spotify-ELT-Pipeline-Analytics/feature_music.csv","feature_music")