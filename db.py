import json
import os
import re
import tempfile
from urllib.request import Request, urlopen

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

API_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")
TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE", "killrvideo")

# Extract DB ID from the API endpoint URL
DB_ID = re.match(r"https://([0-9a-f-]{36})", API_ENDPOINT).group(1)

_session = None


def _download_bundle():
    """Download secure connect bundle from Astra DevOps API."""
    url = f"https://api.astra.datastax.com/v2/databases/{DB_ID}"
    req = Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    data = json.loads(urlopen(req).read().decode())
    bundle_url = data["info"]["datacenters"][0]["secureBundleUrl"]

    bundle_path = os.path.join(tempfile.gettempdir(), f"secure-connect-{DB_ID}.zip")
    with urlopen(Request(bundle_url)) as r, open(bundle_path, "wb") as f:
        f.write(r.read())
    return bundle_path


def get_session():
    """Return a cached Cassandra session, connecting on first call."""
    global _session
    if _session is None:
        bundle_path = _download_bundle()
        cloud_config = {"secure_connect_bundle": bundle_path}
        auth = PlainTextAuthProvider("token", TOKEN)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth)
        _session = cluster.connect()
        _session.set_keyspace(KEYSPACE)
    return _session
