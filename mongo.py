# pip install pymongo

import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne

# Wenn du das Script am HOST laufen lässt:
#   mongodb://localhost:27017
# Wenn du das Script als Container im selben docker-compose Netzwerk laufen lässt:
#   mongodb://mongo:27017
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = os.getenv("MONGO_DB", "willhaben")
COLL_NAME = os.getenv("MONGO_COLLECTION", "immobilien")

def get_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    col = db[COLL_NAME]

    # Unique per Listing (damit du nicht doppelt speicherst)
    col.create_index("url", unique=True)

    # Optional: schnelle Sortierung / Queries
    col.create_index("scraped_at")

    return col

def save_items_to_mongo(items: list[dict]) -> dict:
    if not items:
        return {"matched": 0, "upserted": 0, "modified": 0}

    col = get_collection()
    now = datetime.now(timezone.utc)

    ops = []
    for item in items:
        url = item.get("url")
        if not url:
            continue

        doc = {**item, "scraped_at": now}

        ops.append(
            UpdateOne(
                {"url": url},
                {
                    "$set": doc,
                    "$setOnInsert": {"first_seen_at": now},
                },
                upsert=True,
            )
        )

    if not ops:
        return {"matched": 0, "upserted": 0, "modified": 0}

    res = col.bulk_write(ops, ordered=False)

    return {
        "matched": res.matched_count,
        "upserted": len(res.upserted_ids or {}),
        "modified": res.modified_count,
    }
