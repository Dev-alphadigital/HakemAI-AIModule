"""
Activity Logs Service
=====================
Production-ready service for tracking and retrieving user activity logs.

Features:
- User activity tracking
- Login/logout logging
- Action logging with metadata
- Filtering and pagination
- Statistics and analytics
- Real-time activity monitoring

Author: Production Implementation
Date: 2025
"""

import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
from bson import ObjectId
from collections import defaultdict

logger = logging.getLogger(__name__)


class ActivityLogsService:
    """
    Production-ready service for managing activity logs.
    """
    
    def __init__(self):
        self.client = None
        self.database = None
        self.activity_logs_collection = None
    
    async def connect(self):
        """Connect to MongoDB and initialize collections"""
        try:
            mongodb_url = os.getenv("MONGO_URI") or os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            database_name = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB_NAME", "hakemAI")
            
            logger.info(f"🔌 Activity Logs Service: Connecting to MongoDB")
            
            self.client = AsyncIOMotorClient(mongodb_url)
            self.database = self.client[database_name]
            self.activity_logs_collection = self.database.activity_logs
            
            # Test connection
            await self.client.admin.command("ping")
            logger.info("✅ Activity Logs Service: Successfully connected to MongoDB")
            
            # Create indexes
            await self._create_indexes()
            
        except ConnectionFailure as e:
            logger.error(f"❌ Activity Logs Service: Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Activity Logs Service: Unexpected error: {e}")
            raise
    
    async def _create_indexes(self):
        """Create database indexes for optimal performance"""
        try:
            # Index on userId for fast user-specific queries
            await self.activity_logs_collection.create_index("userId")
            
            # Index on activityType for filtering
            await self.activity_logs_collection.create_index("activityType")
            
            # Index on createdAt for sorting and date range queries
            await self.activity_logs_collection.create_index("createdAt")
            
            # Compound index for user + date queries
            await self.activity_logs_collection.create_index([("userId", 1), ("createdAt", -1)])
            
            # Compound index for activity type + date
            await self.activity_logs_collection.create_index([("activityType", 1), ("createdAt", -1)])
            
            # Text index for search functionality
            await self.activity_logs_collection.create_index([
                ("username", "text"),
                ("userEmail", "text"),
                ("description", "text")
            ])
            
            logger.info("✅ Activity Logs Service: Indexes created successfully")
            
        except Exception as e:
            logger.error(f"❌ Activity Logs Service: Error creating indexes: {e}")
    
    async def create_activity_log(
        self,
        user_id: str,
        activity_type: str,
        description: str,
        user_email: Optional[str] = None,
        username: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new activity log entry.
        
        Args:
            user_id: User ID
            activity_type: Type of activity (e.g., "login", "logout", "comparison_created", etc.)
            description: Human-readable description
            user_email: User email (optional)
            username: Username (optional)
            ip_address: IP address (optional)
            user_agent: User agent string (optional)
            metadata: Additional metadata (optional)
            
        Returns:
            ObjectId string of created log entry
        """
        try:
            log_entry = {
                "userId": user_id,
                "userEmail": user_email or "",
                "username": username or "",
                "activityType": activity_type,
                "description": description,
                "ipAddress": ip_address or "",
                "userAgent": user_agent or "",
                "metadata": metadata or {},
                "createdAt": datetime.utcnow()
            }
            
            result = await self.activity_logs_collection.insert_one(log_entry)
            logger.debug(f"✅ Activity log created: {result.inserted_id} - {activity_type}")
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"❌ Error creating activity log: {e}")
            raise
    
    async def get_activity_logs(
        self,
        page: int = 1,
        limit: int = 50,
        user_id: Optional[str] = None,
        activity_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get activity logs with filtering and pagination.
        
        Args:
            page: Page number (1-based)
            limit: Number of logs per page
            user_id: Filter by user ID
            activity_type: Filter by activity type
            start_date: Start date filter
            end_date: End date filter
            search: Search in username, email, description
            
        Returns:
            Dict with logs, total, page, totalPages
        """
        try:
            # First, check total documents in collection (for debugging)
            total_in_collection = await self.activity_logs_collection.count_documents({})
            logger.info(f"📊 Total activity logs in collection: {total_in_collection}")
            
            # Build query
            query = {}
            
            if user_id:
                # Convert string user_id to ObjectId for proper matching
                try:
                    from bson import ObjectId
                    query["userId"] = ObjectId(user_id)
                    logger.debug(f"🔍 Filtering by userId (ObjectId): {user_id}")
                except Exception:
                    # If conversion fails, try as string (for backward compatibility)
                    query["userId"] = user_id
                    logger.debug(f"🔍 Filtering by userId (string): {user_id}")
            
            if activity_type and activity_type.lower() != "all types":
                query["activityType"] = activity_type
                logger.debug(f"🔍 Filtering by activityType: {activity_type}")
            
            if start_date or end_date:
                query["createdAt"] = {}
                if start_date:
                    query["createdAt"]["$gte"] = start_date
                if end_date:
                    query["createdAt"]["$lte"] = end_date
                logger.debug(f"🔍 Filtering by date range: {start_date} to {end_date}")
            
            # Handle search - use regex instead of $text for more reliable searching
            if search:
                search_regex = {"$regex": search, "$options": "i"}
                query["$or"] = [
                    {"username": search_regex},
                    {"userEmail": search_regex},
                    {"description": search_regex}
                ]
                logger.debug(f"🔍 Searching for: {search}")
            
            logger.debug(f"🔍 Final query: {query}")
            
            # Get total count
            total = await self.activity_logs_collection.count_documents(query)
            logger.info(f"📊 Activity logs matching query: {total}")
            
            # Calculate pagination
            skip = (page - 1) * limit
            total_pages = (total + limit - 1) // limit if total > 0 else 1
            
            # Fetch logs
            cursor = self.activity_logs_collection.find(query).sort("createdAt", -1).skip(skip).limit(limit)
            logs = await cursor.to_list(length=limit)
            
            # Format logs
            formatted_logs = []
            for log in logs:
                # Convert userId ObjectId to string if needed
                user_id = log.get("userId", "")
                if user_id and hasattr(user_id, '__str__'):
                    user_id = str(user_id)
                
                formatted_logs.append({
                    "_id": str(log.get("_id", "")),
                    "userId": user_id,
                    "userEmail": log.get("userEmail", ""),
                    "username": log.get("username", ""),
                    "activityType": log.get("activityType", ""),
                    "description": log.get("description", ""),
                    "ipAddress": log.get("ipAddress", ""),
                    "userAgent": log.get("userAgent", ""),
                    "createdAt": log.get("createdAt").isoformat() if log.get("createdAt") else None,
                    "metadata": log.get("metadata", {})
                })
            
            logger.info(f"📊 Activity logs query result: {len(formatted_logs)} logs found (total: {total})")
            
            return {
                "logs": formatted_logs,
                "total": total,
                "page": page,
                "totalPages": total_pages
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting activity logs: {e}")
            raise
    
    async def get_user_activity_logs(
        self,
        user_id: str,
        page: int = 1,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Get activity logs for a specific user.
        
        Args:
            user_id: User ID
            page: Page number
            limit: Number of logs per page
            
        Returns:
            Dict with logs, total, page, totalPages
        """
        return await self.get_activity_logs(
            page=page,
            limit=limit,
            user_id=user_id
        )
    
    async def get_activity_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get activity statistics.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            
        Returns:
            Dict with statistics
        """
        try:
            # Check if collection exists and has documents
            if self.activity_logs_collection is None:
                raise Exception("Activity logs collection is not initialized")
            
            # Build date query
            date_query = {}
            if start_date or end_date:
                date_query["createdAt"] = {}
                if start_date:
                    date_query["createdAt"]["$gte"] = start_date
                if end_date:
                    date_query["createdAt"]["$lte"] = end_date
            
            # Total activities
            total_activities = await self.activity_logs_collection.count_documents(date_query)
            
            # Activities by type
            try:
                pipeline = []
                if date_query:
                    pipeline.append({"$match": date_query})
                pipeline.extend([
                    {"$group": {
                        "_id": "$activityType",
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"count": -1}}
                ])
                
                activities_by_type = {}
                async for doc in self.activity_logs_collection.aggregate(pipeline):
                    activity_type = doc.get("_id") or "unknown"
                    activities_by_type[activity_type] = doc.get("count", 0)
            except Exception as type_error:
                logger.warning(f"⚠️  Error getting activities by type: {type_error}")
                activities_by_type = {}
            
            # Top users
            try:
                pipeline = []
                if date_query:
                    pipeline.append({"$match": date_query})
                pipeline.extend([
                    {"$group": {
                        "_id": {"userId": "$userId", "username": "$username"},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"count": -1}},
                    {"$limit": 10}
                ])
                
                top_users = []
                async for doc in self.activity_logs_collection.aggregate(pipeline):
                    user_id_obj = doc.get("_id", {})
                    # Convert userId ObjectId to string if needed
                    user_id = user_id_obj.get("userId", "") if isinstance(user_id_obj, dict) else ""
                    if user_id and hasattr(user_id, '__str__'):
                        user_id = str(user_id)
                    
                    username = user_id_obj.get("username", "") if isinstance(user_id_obj, dict) else ""
                    
                    top_users.append({
                        "userId": user_id,
                        "username": username,
                        "count": doc.get("count", 0)
                    })
            except Exception as user_error:
                logger.warning(f"⚠️  Error getting top users: {user_error}")
                import traceback
                logger.error(traceback.format_exc())
                top_users = []
            
            # Activities by day (last 30 days if no date range specified)
            if not start_date:
                start_date = datetime.utcnow() - timedelta(days=30)
            if not end_date:
                end_date = datetime.utcnow()
            
            # Build activities by day pipeline with better error handling
            try:
                # Try using $dateToString first (without timezone for compatibility)
                pipeline = [
                    {"$match": {
                        "createdAt": {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                    }},
                    {"$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$createdAt"
                            }
                        },
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"_id": 1}}
                ]
                
                activities_by_day = []
                async for doc in self.activity_logs_collection.aggregate(pipeline):
                    activities_by_day.append({
                        "date": doc.get("_id", ""),
                        "count": doc.get("count", 0)
                    })
            except Exception as day_error:
                logger.warning(f"⚠️  Error getting activities by day with $dateToString, using fallback: {day_error}")
                import traceback
                logger.error(traceback.format_exc())
                # Fallback: group by day manually if $dateToString fails
                activities_by_day = []
                try:
                    cursor = self.activity_logs_collection.find({
                        "createdAt": {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                    })
                    day_counts = defaultdict(int)
                    async for doc in cursor:
                        if "createdAt" in doc:
                            created_at = doc["createdAt"]
                            if isinstance(created_at, datetime):
                                day_str = created_at.strftime("%Y-%m-%d")
                            elif hasattr(created_at, 'date'):
                                day_str = created_at.date().isoformat()
                            else:
                                # Try to parse as string
                                day_str = str(created_at)[:10]
                            day_counts[day_str] += 1
                    
                    for day, count in sorted(day_counts.items()):
                        activities_by_day.append({"date": day, "count": count})
                except Exception as fallback_error:
                    logger.error(f"❌ Fallback method also failed: {fallback_error}")
                    activities_by_day = []
            
            logger.info(f"📊 Statistics calculated: total={total_activities}, types={len(activities_by_type)}, top_users={len(top_users)}, days={len(activities_by_day)}")
            
            return {
                "totalActivities": total_activities,
                "activitiesByType": activities_by_type,
                "topUsers": top_users,
                "activitiesByDay": activities_by_day
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting activity statistics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Return empty statistics instead of raising to prevent 500 error
            return {
                "totalActivities": 0,
                "activitiesByType": {},
                "topUsers": [],
                "activitiesByDay": []
            }
    
    async def get_recent_activity_logs(
        self,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get recent activity logs.
        
        Args:
            limit: Number of recent logs to return
            
        Returns:
            List of recent activity logs
        """
        try:
            cursor = self.activity_logs_collection.find().sort("createdAt", -1).limit(limit)
            logs = await cursor.to_list(length=limit)
            
            formatted_logs = []
            for log in logs:
                formatted_logs.append({
                    "_id": str(log.get("_id", "")),
                    "userId": log.get("userId", ""),
                    "userEmail": log.get("userEmail", ""),
                    "username": log.get("username", ""),
                    "activityType": log.get("activityType", ""),
                    "description": log.get("description", ""),
                    "ipAddress": log.get("ipAddress", ""),
                    "userAgent": log.get("userAgent", ""),
                    "createdAt": log.get("createdAt").isoformat() if log.get("createdAt") else None,
                    "metadata": log.get("metadata", {})
                })
            
            return formatted_logs
            
        except Exception as e:
            logger.error(f"❌ Error getting recent activity logs: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        try:
            if self.client:
                self.client.close()
                logger.info("✅ Activity Logs Service: Disconnected from MongoDB")
        except Exception as e:
            logger.warning(f"⚠️  Activity Logs Service: Error disconnecting: {e}")


# Global singleton instance
activity_logs_service = ActivityLogsService()

