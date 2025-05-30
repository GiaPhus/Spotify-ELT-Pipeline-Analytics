import os
from dotenv import load_dotenv

dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../.env'))
load_dotenv(dotenv_path)

mongo_uri = os.getenv("MONGODB_USER")
print("Mongo URI:", mongo_uri)
