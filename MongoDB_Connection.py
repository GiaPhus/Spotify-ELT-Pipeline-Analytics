from pymongo import MongoClient

class MongoDB:
    def __init__(self, uri):
        """Initialize MongoDB connection."""
        self.uri = uri
        self.client = MongoClient(self.uri)

    def check_database_exists(self, db_name):
        """Check if a database exists."""
        return db_name in self.client.list_database_names()

    def create_database(self, db_name):
        """Create a database by accessing it (MongoDB creates it when data is inserted)."""
        db = self.client[db_name]
        print(f"Database '{db_name}' created successfully.")
        return db

    def insert_one(self, db_name, collection_name, data):
        """Insert one document into a collection."""
        db = self.client[db_name]
        result = db[collection_name].insert_one(data)
        print(f"Inserted document with ID: {result.inserted_id}")
        return result

    def insert_many(self, db_name, collection_name, data_list):
        """Insert multiple documents into a collection."""
        db = self.client[db_name]
        result = db[collection_name].insert_many(data_list)
        print(f"Inserted {len(result.inserted_ids)} documents.")
        return result

    def create_collection(self, db_name, collection_name):
        """Create a new collection."""
        db = self.client[db_name]
        collection = db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created successfully.")
        return collection

    def check_collection_exists(self, db_name, collection_name):
        """Check if a collection exists in a database."""
        db = self.client[db_name]
        return collection_name in db.list_collection_names()

