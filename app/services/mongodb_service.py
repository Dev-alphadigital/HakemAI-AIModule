import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
from bson import ObjectId

logger = logging.getLogger(__name__)


class EnhancedMongoDBService:
    """
    Enhanced MongoDB service for storing individual PDF extractions
    and comparison results separately.
    """
    
    def __init__(self):
        self.client = None
        self.database = None
        
        # Collections
        self.individual_pdfs_collection = None  # NEW: Individual PDF extractions
        self.comparisons_collection = None       # Final comparison results
        
    async def connect(self):
        """Connect to MongoDB and initialize collections"""
        try:
            # Get MongoDB connection string from environment
            mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            database_name = os.getenv("MONGODB_DATABASE", "hakemAI")

            logger.info(f"🔌 Connecting to MongoDB: {mongodb_url}")
            
            self.client = AsyncIOMotorClient(mongodb_url)
            self.database = self.client[database_name]
            
            # Initialize collections
            self.individual_pdfs_collection = self.database.individual_pdf_extractions
            self.comparisons_collection = self.database.comparisons
            
            # Test connection
            await self.client.admin.command('ping')
            logger.info("✅ Successfully connected to MongoDB")
            
            # Create indexes
            await self._create_indexes()
            
        except ConnectionFailure as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to MongoDB: {e}")
            raise
    
    async def _create_indexes(self):
        """Create database indexes for better performance"""
        try:
            # ===== INDIVIDUAL PDFs COLLECTION INDEXES =====
            
            # Index on comparison_id (for grouping PDFs)
            await self.individual_pdfs_collection.create_index("comparison_id")
            
            # Index on file_name (for searching)
            await self.individual_pdfs_collection.create_index("file_name")
            
            # Index on company_name (for filtering)
            await self.individual_pdfs_collection.create_index("company_name")
            
            # Compound index on comparison_id + file_name (unique per comparison)
            await self.individual_pdfs_collection.create_index(
                [("comparison_id", 1), ("file_name", 1)], 
                unique=True
            )
            
            # Index on extraction_timestamp (for sorting)
            await self.individual_pdfs_collection.create_index(
                "extraction_timestamp", 
                expireAfterSeconds=2592000  # 30 days TTL
            )
            
            # ===== COMPARISONS COLLECTION INDEXES =====
            
            # Unique index on comparison_id
            await self.comparisons_collection.create_index("comparison_id", unique=True)
            
            # Index on created_at (for sorting)
            await self.comparisons_collection.create_index(
                "created_at",
                expireAfterSeconds=2592000  # 30 days TTL
            )
            
            # Index on status
            await self.comparisons_collection.create_index("status")
            
            logger.info("✅ Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"⚠️  Error creating indexes: {e}")
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 Disconnected from MongoDB")
    
    # =========================================================================
    # INDIVIDUAL PDF STORAGE METHODS
    # =========================================================================
    
    async def save_individual_pdf_extraction(
        self,
        comparison_id: str,
        file_name: str,
        original_filename: str,
        file_path: str,
        file_size: int,
        extracted_data: Dict[str, Any]
    ) -> str:
        """
        Save individual PDF extraction data to MongoDB.
        
        Args:
            comparison_id: ID linking multiple PDFs together
            file_name: Unique file name (UUID-based)
            original_filename: Original uploaded filename
            file_path: Path where file is stored
            file_size: Size of file in bytes
            extracted_data: Complete extracted data from AI parser
            
        Returns:
            MongoDB document ID
        """
        try:
            # Prepare document
            document = {
                "comparison_id": comparison_id,
                "file_name": file_name,
                "original_filename": original_filename,
                "file_path": file_path,
                "file_size": file_size,
                
                # Extracted data
                "company_name": extracted_data.get("company_name", "Unknown"),
                "policy_type":  extracted_data.get("policy_type", "Unknown"),
                "premium_amount": extracted_data.get("premium_amount"),
                "rate": extracted_data.get("rate"),
                "score": extracted_data.get("score"),
                
                # Complete extracted data
                "extracted_data": extracted_data,
                
                # Metadata
                "extraction_timestamp": datetime.now(),
                "extraction_status": "success",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            # Insert document
            result = await self.individual_pdfs_collection.insert_one(document)
            
            logger.info(f"✅ Saved individual PDF: {file_name} → {result.inserted_id}")
            return str(result.inserted_id)
            
        except DuplicateKeyError:
            logger.error(f"❌ PDF already exists: {comparison_id}/{file_name}")
            raise ValueError(f"PDF already stored: {file_name}")
        except Exception as e:
            logger.error(f"❌ Error saving individual PDF: {e}")
            raise
    
    async def get_individual_pdf_by_filename(
        self,
        comparison_id: str,
        file_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get individual PDF extraction by filename.
        
        Args:
            comparison_id: Comparison ID
            file_name: File name
            
        Returns:
            PDF extraction data or None
        """
        try:
            document = await self.individual_pdfs_collection.find_one({
                "comparison_id": comparison_id,
                "file_name": file_name
            })
            
            if document:
                document["id"] = str(document["_id"])
                del document["_id"]
                return document
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting PDF by filename: {e}")
            raise
    
    async def get_all_pdfs_for_comparison(
        self,
        comparison_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all individual PDFs for a specific comparison.
        
        Args:
            comparison_id: Comparison ID
            
        Returns:
            List of PDF extraction documents
        """
        try:
            cursor = self.individual_pdfs_collection.find({
                "comparison_id": comparison_id
            }).sort("extraction_timestamp", 1)
            
            documents = []
            async for document in cursor:
                document["id"] = str(document["_id"])
                del document["_id"]
                documents.append(document)
            
            logger.info(f"📁 Retrieved {len(documents)} PDFs for comparison: {comparison_id}")
            return documents
            
        except Exception as e:
            logger.error(f"❌ Error getting PDFs for comparison: {e}")
            raise
    
    async def get_pdfs_by_company(
        self,
        company_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get all PDFs from a specific company.
        
        Args:
            company_name: Company name to search
            limit: Maximum results
            
        Returns:
            List of PDF extraction documents
        """
        try:
            cursor = self.individual_pdfs_collection.find({
                "company_name": {"$regex": company_name, "$options": "i"}
            }).sort("extraction_timestamp", -1).limit(limit)
            
            documents = []
            async for document in cursor:
                document["id"] = str(document["_id"])
                del document["_id"]
                documents.append(document)
            
            return documents
            
        except Exception as e:
            logger.error(f"❌ Error getting PDFs by company: {e}")
            raise
    
    async def delete_individual_pdf(
        self,
        comparison_id: str,
        file_name: str
    ) -> bool:
        """
        Delete an individual PDF extraction.
        
        Args:
            comparison_id: Comparison ID
            file_name: File name
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            result = await self.individual_pdfs_collection.delete_one({
                "comparison_id": comparison_id,
                "file_name": file_name
            })
            
            if result.deleted_count > 0:
                logger.info(f"🗑️  Deleted PDF: {file_name}")
                return True
            else:
                logger.warning(f"⚠️  PDF not found for deletion: {file_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error deleting PDF: {e}")
            raise
    
    # =========================================================================
    # COMPARISON RESULTS STORAGE METHODS
    # =========================================================================
    
    async def save_comparison_result(
        self,
        comparison_id: str,
        comparison_data: Dict[str, Any],
        pdf_count: int
    ) -> str:
        """
        Save final comparison result (combining all PDFs).
        
        Args:
            comparison_id: Unique comparison ID
            comparison_data: Complete comparison result
            pdf_count: Number of PDFs compared
            
        Returns:
            MongoDB document ID
        """
        try:
            # Prepare document
            document = {
                "comparison_id": comparison_id,
                "status": "completed",
                "total_pdfs": pdf_count,
                
                # Complete comparison data
                "comparison_data": comparison_data,
                
                # Summary sections
                "summary": comparison_data.get("summary", {}),
                "key_differences": comparison_data.get("key_differences", {}),
                "side_by_side": comparison_data.get("side_by_side", {}),
                "data_table": comparison_data.get("data_table", {}),
                "charts": comparison_data.get("charts", {}),
                "analytics": comparison_data.get("analytics", {}),
                "provider_cards": comparison_data.get("provider_cards", []),
                
                # Metadata
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "files_processed": comparison_data.get("files_processed", [])
            }
            
            # Insert or update
            result = await self.comparisons_collection.update_one(
                {"comparison_id": comparison_id},
                {"$set": document},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"✅ Saved new comparison: {comparison_id}")
                return str(result.upserted_id)
            else:
                logger.info(f"✅ Updated comparison: {comparison_id}")
                # Get the existing document ID
                existing = await self.comparisons_collection.find_one(
                    {"comparison_id": comparison_id}
                )
                return str(existing["_id"]) if existing else None
            
        except Exception as e:
            logger.error(f"❌ Error saving comparison result: {e}")
            raise
    
    async def get_comparison_result(
        self,
        comparison_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comparison result by ID.
        
        Args:
            comparison_id: Comparison ID
            
        Returns:
            Comparison data or None
        """
        try:
            document = await self.comparisons_collection.find_one({
                "comparison_id": comparison_id
            })
            
            if document:
                document["id"] = str(document["_id"])
                del document["_id"]
                return document
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting comparison result: {e}")
            raise
    
    async def get_recent_comparisons(
        self,
        limit: int = 10,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get recent comparison results.
        
        Args:
            limit: Maximum results
            skip: Number to skip (pagination)
            
        Returns:
            List of comparison documents
        """
        try:
            cursor = self.comparisons_collection.find().sort(
                "created_at", -1
            ).skip(skip).limit(limit)
            
            documents = []
            async for document in cursor:
                document["id"] = str(document["_id"])
                del document["_id"]
                
                # Remove large fields for list view
                document.pop("comparison_data", None)
                
                documents.append(document)
            
            return documents
            
        except Exception as e:
            logger.error(f"❌ Error getting recent comparisons: {e}")
            raise
    
    async def delete_comparison_result(
        self,
        comparison_id: str
    ) -> bool:
        """
        Delete a comparison result.
        
        Args:
            comparison_id: Comparison ID
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            result = await self.comparisons_collection.delete_one({
                "comparison_id": comparison_id
            })
            
            if result.deleted_count > 0:
                logger.info(f"🗑️  Deleted comparison: {comparison_id}")
                return True
            else:
                logger.warning(f"⚠️  Comparison not found: {comparison_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error deleting comparison: {e}")
            raise
    
    # =========================================================================
    # COMBINED OPERATIONS
    # =========================================================================
    
    async def get_complete_comparison_with_pdfs(
        self,
        comparison_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get complete comparison including all individual PDFs.
        
        Args:
            comparison_id: Comparison ID
            
        Returns:
            Complete comparison data with individual PDFs
        """
        try:
            # Get comparison result
            comparison = await self.get_comparison_result(comparison_id)
            
            if not comparison:
                return None
            
            # Get all individual PDFs
            individual_pdfs = await self.get_all_pdfs_for_comparison(comparison_id)
            
            # Combine
            result = {
                **comparison,
                "individual_pdfs": individual_pdfs,
                "pdf_count": len(individual_pdfs)
            }
            
            logger.info(f"📦 Retrieved complete comparison: {comparison_id} ({len(individual_pdfs)} PDFs)")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting complete comparison: {e}")
            raise
    
    async def delete_complete_comparison(
        self,
        comparison_id: str
    ) -> Dict[str, int]:
        """
        Delete comparison result AND all associated individual PDFs.
        
        Args:
            comparison_id: Comparison ID
            
        Returns:
            Dict with deletion counts
        """
        try:
            # Delete all individual PDFs
            pdf_result = await self.individual_pdfs_collection.delete_many({
                "comparison_id": comparison_id
            })
            
            # Delete comparison result
            comp_result = await self.comparisons_collection.delete_one({
                "comparison_id": comparison_id
            })
            
            logger.info(
                f"🗑️  Deleted comparison {comparison_id}: "
                f"{pdf_result.deleted_count} PDFs, "
                f"{comp_result.deleted_count} comparison"
            )
            
            return {
                "pdfs_deleted": pdf_result.deleted_count,
                "comparison_deleted": comp_result.deleted_count
            }
            
        except Exception as e:
            logger.error(f"❌ Error deleting complete comparison: {e}")
            raise
    
    # =========================================================================
    # STATISTICS & ANALYTICS
    # =========================================================================
    
    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Statistics dictionary
        """
        try:
            total_pdfs = await self.individual_pdfs_collection.count_documents({})
            total_comparisons = await self.comparisons_collection.count_documents({})
            
            # Get top companies
            pipeline = [
                {"$group": {
                    "_id": "$company_name",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            
            top_companies = []
            async for doc in self.individual_pdfs_collection.aggregate(pipeline):
                top_companies.append({
                    "company": doc["_id"],
                    "pdf_count": doc["count"]
                })
            
            # Get recent activity
            recent_pdfs = await self.individual_pdfs_collection.count_documents({
                "extraction_timestamp": {
                    "$gte": datetime.now().replace(hour=0, minute=0, second=0)
                }
            })
            
            return {
                "total_pdfs_stored": total_pdfs,
                "total_comparisons": total_comparisons,
                "top_companies": top_companies,
                "pdfs_today": recent_pdfs,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting statistics: {e}")
            return {"error": str(e)}


# Global instance
enhanced_mongodb_service = EnhancedMongoDBService()