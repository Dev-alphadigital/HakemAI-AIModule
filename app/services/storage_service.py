"""
Storage Service
===============
Handles MongoDB storage for comparison results and metadata.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from app.core.config import settings

logger = logging.getLogger(__name__)


class StorageService:
    """Service for storing and retrieving comparison data from MongoDB"""
    
    def __init__(self):
        """Initialize MongoDB connection"""
        self.client = None
        self.db = None
        self.collection = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize MongoDB connection"""
        # Check if storage is enabled
        if not getattr(settings, 'ENABLE_STORAGE', False):
            logger.info("MongoDB storage is disabled via ENABLE_STORAGE setting")
            self.client = None
            self.db = None
            self.collection = None
            return
            
        try:
            # Get MongoDB connection string from environment
            mongodb_url = getattr(settings, 'MONGODB_URL', 'mongodb://localhost:27017')
            database_name = getattr(settings, 'MONGODB_DATABASE', 'hakemAI')
            collection_name = getattr(settings, 'MONGODB_COLLECTION', 'comparisons')
            
            # Connect to MongoDB
            self.client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5000)
            
            # Test connection
            self.client.admin.command('ping')
            
            # Get database and collection
            self.db = self.client[database_name]
            self.collection = self.db[collection_name]
            
            # Create indexes for better performance
            self._create_indexes()
            
            logger.info(f"Connected to MongoDB: {database_name}.{collection_name}")
            
        except ConnectionFailure as e:
            logger.warning(f"MongoDB connection failed: {str(e)}")
            self.client = None
            self.db = None
            self.collection = None
        except Exception as e:
            logger.error(f"Error initializing MongoDB: {str(e)}")
            self.client = None
            self.db = None
            self.collection = None
    
    def _create_indexes(self):
        """Create database indexes for better performance"""
        try:
            if self.collection is not None:
                # Check if we have enough disk space (MongoDB requires ~500MB for indexes)
                import shutil
                free_space = shutil.disk_usage('.').free
                min_required = 524288000  # 500MB in bytes
                
                if free_space < min_required:
                    logger.warning(f"Insufficient disk space for MongoDB indexes. Available: {free_space} bytes, Required: {min_required} bytes. Skipping index creation.")
                    return
                
                # Index on timestamp for recent queries
                self.collection.create_index([("timestamp", -1)])
                # Index on company names for search
                self.collection.create_index([("companies", 1)])
                # Index on comparison_id for lookups
                self.collection.create_index([("comparison_id", 1)], unique=True)
                logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {str(e)}")
    
    async def save_comparison(self, comparison_data: Dict[str, Any], metadata: Dict[str, Any] = None) -> Optional[str]:
        """
        Save a comparison result to MongoDB
        
        Args:
            comparison_data: The comparison result data
            metadata: Additional metadata about the comparison
            
        Returns:
            MongoDB document ID if successful, None otherwise
        """
        if self.collection is None:
            logger.warning("MongoDB not available, skipping save")
            return None
        
        try:
            # Prepare document
            document = {
                "comparison_id": self._generate_comparison_id(),
                "timestamp": datetime.utcnow(),
                "data": comparison_data,
                "metadata": metadata or {},
                "companies": self._extract_company_names(comparison_data),
                "total_quotes": comparison_data.get("total_quotes", 0),
                "status": "completed"
            }
            
            # Insert document
            result = self.collection.insert_one(document)
            
            if result.inserted_id:
                logger.info(f"Saved comparison to MongoDB: {result.inserted_id}")
                return str(result.inserted_id)
            else:
                logger.error("Failed to save comparison to MongoDB")
                return None
                
        except Exception as e:
            logger.error(f"Error saving comparison to MongoDB: {str(e)}")
            return None
    
    async def get_comparison_by_id(self, comparison_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a comparison by its MongoDB ID
        
        Args:
            comparison_id: MongoDB document ID
            
        Returns:
            Comparison data if found, None otherwise
        """
        if self.collection is None:
            logger.warning("MongoDB not available")
            return None
        
        try:
            from bson import ObjectId
            
            # Try to convert to ObjectId
            try:
                object_id = ObjectId(comparison_id)
            except:
                # If not a valid ObjectId, search by comparison_id field
                document = self.collection.find_one({"comparison_id": comparison_id})
            else:
                document = self.collection.find_one({"_id": object_id})
            
            if document:
                # Convert ObjectId to string for JSON serialization
                document["_id"] = str(document["_id"])
                return document
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving comparison: {str(e)}")
            return None
    
    async def get_recent_comparisons(self, limit: int = 10, skip: int = 0) -> List[Dict[str, Any]]:
        """
        Get recent comparison results
        
        Args:
            limit: Maximum number of results
            skip: Number of results to skip
            
        Returns:
            List of recent comparisons
        """
        if self.collection is None:
            logger.warning("MongoDB not available")
            return []
        
        try:
            cursor = self.collection.find().sort("timestamp", -1).skip(skip).limit(limit)
            results = []
            
            for document in cursor:
                # Convert ObjectId to string
                document["_id"] = str(document["_id"])
                results.append(document)
            
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving recent comparisons: {str(e)}")
            return []
    
    async def search_comparisons_by_company(self, company_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search comparisons by company name
        
        Args:
            company_name: Company name to search for
            limit: Maximum number of results
            
        Returns:
            List of matching comparisons
        """
        if self.collection is None:
            logger.warning("MongoDB not available")
            return []
        
        try:
            # Case-insensitive search
            query = {"companies": {"$regex": company_name, "$options": "i"}}
            cursor = self.collection.find(query).sort("timestamp", -1).limit(limit)
            results = []
            
            for document in cursor:
                # Convert ObjectId to string
                document["_id"] = str(document["_id"])
                results.append(document)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching comparisons: {str(e)}")
            return []
    
    async def get_comparison_stats(self) -> Dict[str, Any]:
        """
        Get statistics about stored comparisons
        
        Returns:
            Statistics dictionary
        """
        if self.collection is None:
            logger.warning("MongoDB not available")
            return {"error": "MongoDB not available"}
        
        try:
            # Get total count
            total_count = self.collection.count_documents({})
            
            # Get date range
            oldest = self.collection.find().sort("timestamp", 1).limit(1)
            newest = self.collection.find().sort("timestamp", -1).limit(1)
            
            oldest_date = None
            newest_date = None
            
            for doc in oldest:
                oldest_date = doc.get("timestamp")
                break
            
            for doc in newest:
                newest_date = doc.get("timestamp")
                break
            
            # Get top companies
            pipeline = [
                {"$unwind": "$companies"},
                {"$group": {"_id": "$companies", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            
            top_companies = list(self.collection.aggregate(pipeline))
            
            return {
                "total_comparisons": total_count,
                "oldest_comparison": oldest_date.isoformat() if oldest_date else None,
                "newest_comparison": newest_date.isoformat() if newest_date else None,
                "top_companies": top_companies
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
            return {"error": str(e)}
    
    async def delete_comparison(self, comparison_id: str) -> bool:
        """
        Delete a comparison by its ID
        
        Args:
            comparison_id: MongoDB document ID
            
        Returns:
            True if deleted, False otherwise
        """
        if self.collection is None:
            logger.warning("MongoDB not available")
            return False
        
        try:
            from bson import ObjectId
            
            # Try to convert to ObjectId
            try:
                object_id = ObjectId(comparison_id)
                result = self.collection.delete_one({"_id": object_id})
            except:
                # If not a valid ObjectId, search by comparison_id field
                result = self.collection.delete_one({"comparison_id": comparison_id})
            
            return result.deleted_count > 0
            
        except Exception as e:
            logger.error(f"Error deleting comparison: {str(e)}")
            return False
    
    def _generate_comparison_id(self) -> str:
        """Generate a unique comparison ID"""
        import uuid
        return str(uuid.uuid4())
    
    def _extract_company_names(self, comparison_data: Dict[str, Any]) -> List[str]:
        """Extract company names from comparison data"""
        companies = []
        
        try:
            ranking = comparison_data.get("ranking", [])
            for quote in ranking:
                if isinstance(quote, dict):
                    company_name = quote.get("company_name")
                    if company_name:
                        companies.append(company_name)
        except Exception as e:
            logger.warning(f"Error extracting company names: {str(e)}")
        
        return companies


# Create singleton instance
storage_service = StorageService()
