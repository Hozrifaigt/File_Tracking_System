import config
import logging
from datetime import datetime
from typing import Dict, Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self):
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None

    async def initialize(self) -> None:
        """Initialize database connection and create collection if not exists"""
        try:
            self._client = AsyncIOMotorClient(config.MONGO_URI)
            self._db = self._client[config.DB_NAME]
            
            # Test connection
            await self._db.command('ping')
            
            # Create collection if it doesn't exist
            collection_name = f"{config.COLLECTION_NAME}_files"
            if collection_name not in await self._db.list_collection_names():
                await self._db.create_collection(collection_name)
                logger.info(f"Created collection: {collection_name}")
            
            logger.info(f"Successfully connected to MongoDB and initialized {collection_name}")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB: {e}")
            raise

    async def close(self) -> None:
        """Close database connection"""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")

    async def test_connection(self) -> bool:
        """Check if database connection is alive"""
        try:
            await self._db.command('ping')
            return True
        except Exception as e:
            logger.error(f"Database ping failed: {e}")
            return False

    @property
    def _files_collection(self):
        """Get the files collection with proper error handling"""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._db[f"{config.COLLECTION_NAME}_files"]


    async def save_scan_results(self, scan_results: dict):
        """Save scan results to results collection"""
        try:
            collection = self._db[config.RESULTS_COLLECTION]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            document = {
                "timestamp": timestamp,
                "results": scan_results
            }
            
            result = await collection.insert_one(document)
            logger.info(f"Successfully saved scan results with ID: {result.inserted_id}")
            return result
        except Exception as e:
            logger.error(f"Failed to save scan results: {e}")
            raise

    async def get_processed_file(self, file_path: str):
        """Get file record from database"""
        collection = self._db[f"{config.COLLECTION_NAME}_files"]
        return await collection.find_one({"file_path": file_path})

    async def get_all_processed_files(self):
        """Get all processed files from database"""
        collection = self._db[f"{config.COLLECTION_NAME}_files"]
        cursor = collection.find({"status": config.STATUS_PROCESSED})
        return await cursor.to_list(length=None)

    async def update_file_status(self, file_path: str, status: str):
        """Update file status in database"""
        collection = self._db[f"{config.COLLECTION_NAME}_files"]
        await collection.update_one(
            {"file_path": file_path},
            {"$set": {"status": status, "updated_at": datetime.utcnow()}}
        )

    async def insert_processed_file(self, file_data: dict):
        """Insert new processed file record"""
        try:
            collection = self._files_collection
            file_data["processed_time"] = datetime.utcnow()
            result = await collection.insert_one(file_data)
            logger.info(f"Successfully inserted document with ID: {result.inserted_id}")
            logger.debug(f"Inserted file data: {file_data}")
            return result
        except Exception as e:
            logger.error(f"Failed to insert file data: {e}")
            logger.error(f"Attempted to insert: {file_data}")
            raise

    async def update_file_status(self, file_path: str, status: str, metadata: dict = None):
        """Update file status and metadata"""
        collection = self._files_collection
        update_data = {
            "status": status,
            "last_updated": datetime.utcnow()
        }
        if metadata:
            update_data.update(metadata)
        
        return await collection.update_one(
            {"file_path": file_path},
            {"$set": update_data}
        )

    async def delete_file_record(self, file_path: str):
        """Delete a file record from database"""
        collection = self._files_collection
        return await collection.delete_one({"file_path": file_path})

    async def get_file_history(self, file_path: str) -> list:
        """Get processing history for a specific file"""
        collection = self._files_collection
        cursor = collection.find(
            {"file_path": file_path},
            sort=[("processed_time", -1)]
        )
        return await cursor.to_list(length=None)

db_handler = DatabaseHandler()