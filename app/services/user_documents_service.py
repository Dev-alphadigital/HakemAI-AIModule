"""
User Documents Service
======================
Stores raw PDF files uploaded by users in MongoDB.
No0 connection to comparison_id - just user documents.
"""
from dotenv import load_dotenv
load_dotenv()
import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
from bson import ObjectId, Binary

logger = logging.getLogger(__name__)


class UserDocumentsService:
    """
    Service for managing user documents (raw PDFs) in MongoDB.
    """

    def __init__(self):
        self.client = None
        self.database = None
        self.documents_collection = None

    async def connect(self):
        """Connect to MongoDB and initialize documents collection"""
        try:
            # Get MongoDB connection string from environment
            # Use the same MongoDB as NestJS backend
            mongodb_url = os.getenv(
                "MONGO_URI", os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            )

            # Extract database name from URI or use environment variable
            database_name = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB_NAME")

            if not database_name:
                # Try to extract from URI
                try:
                    from urllib.parse import urlparse

                    parsed = urlparse(mongodb_url)
                    path = parsed.path.strip("/")

                    if path and path not in ["", "mongodb"]:
                        database_name = path.split("?")[0]
                except Exception:
                    pass

            # Default database name (same as backend)
            if not database_name:
                database_name = "hakemAI"

            logger.info(f"🔌 Connecting User Documents Service to MongoDB")
            logger.info(f"   URL: {mongodb_url}")
            logger.info(f"   Database: {database_name}")

            self.client = AsyncIOMotorClient(mongodb_url)
            self.database = self.client[database_name]
            self.documents_collection = self.database.user_documents

            # Test connection
            await self.client.admin.command("ping")
            logger.info("✅ User Documents Service connected to MongoDB")

        except ConnectionFailure as e:
            logger.error(f"❌ User Documents Service failed to connect: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting User Documents Service: {e}")
            raise

    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 User Documents Service disconnected from MongoDB")

    async def save_document(
        self, user_id: str, original_filename: str, pdf_binary: bytes, file_size: int
    ) -> str:
        """
        Save a raw PDF document for a user.

        Args:
            user_id: User MongoDB ObjectId as string
            original_filename: Original filename
            pdf_binary: Raw PDF binary content
            file_size: Size in bytes

        Returns:
            MongoDB document ID
        """
        try:
            if not ObjectId.is_valid(user_id):
                raise ValueError(f"Invalid user ID format: {user_id}")

            document = {
                "user_id": ObjectId(user_id),
                "original_filename": original_filename,
                "pdf_binary": Binary(pdf_binary),
                "file_size": file_size,
                "file_extension": ".pdf",
                "uploaded_at": datetime.utcnow(),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }

            result = await self.documents_collection.insert_one(document)
            logger.info(
                f"✅ Saved user document: {original_filename} → {result.inserted_id} ({file_size} bytes)"
            )
            return str(result.inserted_id)

        except Exception as e:
            logger.error(f"❌ Error saving user document: {e}")
            raise

    async def get_user_documents(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Get all documents for a user.

        Args:
            user_id: User MongoDB ObjectId as string

        Returns:
            List of document metadata (without binary)
        """
        try:
            if self.documents_collection is None:
                raise ConnectionError("User Documents Service not connected to MongoDB")

            if not ObjectId.is_valid(user_id):
                raise ValueError(f"Invalid user ID format: {user_id}")

            cursor = self.documents_collection.find(
                {"user_id": ObjectId(user_id)},
                {"pdf_binary": 0},  # Exclude binary from list
            ).sort("uploaded_at", -1)

            documents = []
            async for doc in cursor:
                doc["id"] = str(doc["_id"])
                del doc["_id"]
                doc["user_id"] = str(doc["user_id"])
                # Convert datetime to ISO string for JSON serialization
                if "uploaded_at" in doc:
                    doc["uploaded_at"] = (
                        doc["uploaded_at"].isoformat()
                        if hasattr(doc["uploaded_at"], "isoformat")
                        else str(doc["uploaded_at"])
                    )
                if "created_at" in doc:
                    doc["created_at"] = (
                        doc["created_at"].isoformat()
                        if hasattr(doc["created_at"], "isoformat")
                        else str(doc["created_at"])
                    )
                if "updated_at" in doc:
                    doc["updated_at"] = (
                        doc["updated_at"].isoformat()
                        if hasattr(doc["updated_at"], "isoformat")
                        else str(doc["updated_at"])
                    )
                documents.append(doc)

            logger.info(f"📋 Found {len(documents)} documents for user {user_id}")
            return documents

        except Exception as e:
            logger.error(f"❌ Error getting user documents: {e}")
            import traceback

            logger.error(traceback.format_exc())
            raise

    async def get_document_binary(
        self, document_id: str, user_id: str
    ) -> Optional[bytes]:
        """
        Get raw PDF binary for a document.

        Args:
            document_id: Document MongoDB ObjectId as string
            user_id: User MongoDB ObjectId as string (for verification)

        Returns:
            Raw PDF binary data or None
        """
        try:
            if not ObjectId.is_valid(document_id) or not ObjectId.is_valid(user_id):
                return None

            document = await self.documents_collection.find_one(
                {"_id": ObjectId(document_id), "user_id": ObjectId(user_id)},
                {"pdf_binary": 1, "original_filename": 1},
            )

            if document and document.get("pdf_binary"):
                pdf_binary = document["pdf_binary"]
                if isinstance(pdf_binary, Binary):
                    return bytes(pdf_binary)
                elif isinstance(pdf_binary, bytes):
                    return pdf_binary
                return bytes(pdf_binary)

            return None

        except Exception as e:
            logger.error(f"❌ Error getting document binary: {e}")
            raise

    async def delete_document(self, document_id: str, user_id: str) -> bool:
        """
        Delete a document.

        Args:
            document_id: Document MongoDB ObjectId as string
            user_id: User MongoDB ObjectId as string (for verification)

        Returns:
            True if deleted, False if not found
        """
        try:
            if not ObjectId.is_valid(document_id) or not ObjectId.is_valid(user_id):
                return False

            result = await self.documents_collection.delete_one(
                {"_id": ObjectId(document_id), "user_id": ObjectId(user_id)}
            )

            if result.deleted_count > 0:
                logger.info(f"✅ Deleted document {document_id} for user {user_id}")
                return True
            else:
                logger.warning(
                    f"⚠️  Document {document_id} not found for user {user_id}"
                )
                return False

        except Exception as e:
            logger.error(f"❌ Error deleting document: {e}")
            raise


# Global instance
user_documents_service = UserDocumentsService()
