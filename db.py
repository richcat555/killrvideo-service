import os

from astrapy import DataAPIClient
from dotenv import load_dotenv

load_dotenv()

API_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")
TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE", "killrvideo")

_db = None


def get_db():
    """Return a cached astrapy Database object, connecting on first call."""
    global _db
    if _db is None:
        client = DataAPIClient(TOKEN)
        _db = client.get_database(API_ENDPOINT, keyspace=KEYSPACE)
    return _db
