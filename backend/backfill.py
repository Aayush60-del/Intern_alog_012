"""
backfill.py - One-time MongoDB backfill for completeness.

This permanently fills missing/blank fields on existing records:
  - city -> county or "Unknown"
  - phone -> "Not Available"
  - opening_hours -> "Not Available"
  - website -> Google search link (based on name + city/county/state)

Usage:
  python backfill.py --limit 100 --dry-run
  python backfill.py --limit 500
  python backfill.py            # runs on all docs (may take time)
"""

import argparse
import os
import sys
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pymongo import MongoClient


DEFAULT_COUNTRY = "United States"


def build_website_fallback(doc):
    name = (doc.get("name") or "Cemetery").strip()
    location_hint = (
        (doc.get("city") or "").strip()
        or (doc.get("county") or "").strip()
        or (doc.get("state") or "").strip()
        or DEFAULT_COUNTRY
    )
    query = " ".join(part for part in (name, location_hint, "cemetery") if part).strip()
    return f"https://www.google.com/search?q={quote_plus(query)}"


def apply_fallbacks(doc):
    updated = {}

    city = (doc.get("city") or "").strip()
    county = (doc.get("county") or "").strip()
    if not city:
        updated["city"] = county or "Unknown"

    phone = (doc.get("phone") or "").strip()
    if not phone:
        updated["phone"] = "Not Available"

    opening_hours = (doc.get("opening_hours") or "").strip()
    if not opening_hours:
        updated["opening_hours"] = "Not Available"

    website = (doc.get("website") or "").strip()
    if not website:
        updated["website"] = build_website_fallback({**doc, **updated})

    return updated


def get_collection():
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is not set (check backend/.env or environment).")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    db = client["Cemetery_algson"]
    return db["Cemetery_data"]


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Backfill MongoDB cemetery records with fallbacks.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of documents processed (0 = all).")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; do not write updates.")
    args = parser.parse_args()

    collection = get_collection()

    query = {
        "$or": [
            {"city": {"$in": [None, ""]}},
            {"phone": {"$in": [None, ""]}},
            {"opening_hours": {"$in": [None, ""]}},
            {"website": {"$in": [None, ""]}},
        ]
    }

    cursor = collection.find(
        query,
        {
            "_id": 1,
            "name": 1,
            "city": 1,
            "county": 1,
            "state": 1,
            "phone": 1,
            "opening_hours": 1,
            "website": 1,
        },
    )
    if args.limit and args.limit > 0:
        cursor = cursor.limit(args.limit)

    scanned = 0
    changed_docs = 0
    changed_fields = {"city": 0, "phone": 0, "opening_hours": 0, "website": 0}

    for doc in cursor:
        scanned += 1
        updates = apply_fallbacks(doc)
        if not updates:
            continue

        changed_docs += 1
        for key in updates.keys():
            changed_fields[key] = changed_fields.get(key, 0) + 1

        if not args.dry_run:
            collection.update_one({"_id": doc["_id"]}, {"$set": updates})

        if scanned % 250 == 0:
            print(f"Progress: scanned={scanned} updated_docs={changed_docs}")

    mode = "DRY RUN" if args.dry_run else "WRITE"
    print("\nBackfill complete")
    print(f"Mode: {mode}")
    print(f"Scanned: {scanned}")
    print(f"Docs updated: {changed_docs}")
    print("Fields updated:", changed_fields)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

