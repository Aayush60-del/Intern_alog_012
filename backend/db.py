from pymongo import MongoClient
import os

client = None
db = None

class DatabaseNotReadyError(RuntimeError):
    pass

def init_db(app):
    global client, db
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        db = None
        app.logger.error("MONGO_URI is not set. Add it to .env (or set env var MONGO_URI).")
        return
    
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command("ping")

        db_name = os.environ.get("MONGO_DB_NAME", "Cemetery_algson")
        collection_name = os.environ.get("MONGO_COLLECTION", "Cemetery_data")
        db = client[db_name]
        app.logger.info("MongoDB connected")
        
        # Create indexes for performance/queries
        try:
            collection = db[collection_name]
            collection.create_index([("state", 1), ("city", 1)])
            collection.create_index("name")
            collection.create_index(
                [("name", 1), ("latitude", 1), ("longitude", 1)],
                unique=True,
                sparse=True,
            )
            # Geo queries (optional, but cheap and useful).
            collection.create_index([("location", "2dsphere")])
        except Exception as exc:
            app.logger.warning(f"MongoDB index creation skipped: {exc}")
        
    except Exception as e:
        app.logger.error(f"MongoDB connection failed: {e}")
        db = None
        raise

def get_collection():
    if db is None:
        raise DatabaseNotReadyError(
            "Database is not ready. Set `MONGO_URI` (and restart the server)."
        )
    collection_name = os.environ.get("MONGO_COLLECTION", "Cemetery_data")
    return db[collection_name]