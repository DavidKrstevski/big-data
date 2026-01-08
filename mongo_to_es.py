import os
import json
from pathlib import Path

from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")

DB_NAME = os.getenv("MONGO_DB", "willhaben")
COLL_NAME = os.getenv("MONGO_COLLECTION", "immobilien")
INDEX = os.getenv("ES_INDEX", "willhaben_immobilien")

DATA_FILE = os.getenv("ES_DATA_FILE", "snapshots/es_data.jsonl")

BOOL_COLS = [
    "einbauküche", "fahrstuhl", "balkon", "terrasse",
    "garage", "parkplatz", "teilmöbliert_/_möbliert",
]

def ensure_index(es: Elasticsearch):
    if es.indices.exists(index=INDEX):
        return

    es.indices.create(
        index=INDEX,
        mappings={
            "properties": {
                "url": {"type": "keyword"},
                "titel": {"type": "text"},
                "address": {"type": "text"},
                "Bezirk": {"type": "integer"},
                "preis": {"type": "float"},
                "wohnfläche": {"type": "float"},
                "zimmer": {"type": "float"},
                "eur_per_m2": {"type": "float"},
                **{c: {"type": "boolean"} for c in BOOL_COLS},
                "bautyp": {"type": "keyword"},
                "zustand": {"type": "keyword"},
                "scraped_at": {"type": "date"},
                "first_seen_at": {"type": "date"},
            }
        },
    )

def normalize(d: dict) -> dict:
    d.pop("_id", None)

    for c in BOOL_COLS:
        if c in d:
            d[c] = True if d[c] is True or d[c] == 1 else False

    return d

def actions_from_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            d = normalize(d)
            url = d.get("url")
            if not url:
                continue
            yield {
                "_op_type": "index",
                "_index": INDEX,
                "_id": url,
                "_source": d,
            }

def actions_from_mongo():
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    col = mongo[DB_NAME][COLL_NAME]
    for d in col.find({}):
        d = normalize(d)
        url = d.get("url")
        if not url:
            continue
        yield {
            "_op_type": "index",
            "_index": INDEX,
            "_id": url,
            "_source": d,
        }

def main():
    es = Elasticsearch(ES_URL)
    ensure_index(es)

    data_path = Path(DATA_FILE)

    if data_path.exists():
        actions = actions_from_jsonl(data_path)
        source = f"JSONL file: {data_path}"
    else:
        actions = actions_from_mongo()
        source = f"Mongo: {DB_NAME}.{COLL_NAME} @ {MONGO_URI}"

    helpers.bulk(es.options(request_timeout=120), actions, chunk_size=500)
    print(f"Indexed into '{INDEX}' from {source}")

if __name__ == "__main__":
    main()
