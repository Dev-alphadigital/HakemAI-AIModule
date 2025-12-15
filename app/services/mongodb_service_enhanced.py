"""
Enhanced MongoDB Service v2.0
==============================
✅ Stores INDIVIDUAL PDF extractions separately
✅ Links all PDFs to same comparison_id
✅ Stores final comparison result
✅ No data mixing between PDFs
✅ Easy retrieval by file, company, or comparison

Author: Enhanced for individual PDF tracking
Date: October 28, 2025
"""

import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
from bson import ObjectId, Binary

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
        self.comparisons_collection = None  # Final comparison results
        self.companies_collection = None  # Companies collection (for sub-user lookups)

    async def connect(self):
        """Connect to MongoDB and initialize collections"""
        try:
            # Get MongoDB connection string from environment
            mongodb_url = os.getenv("MONGO_URI") or os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            database_name = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB_NAME", "hakemAI")

            logger.info(f"🔌 Connecting to MongoDB: {mongodb_url}")

            self.client = AsyncIOMotorClient(mongodb_url)
            self.database = self.client[database_name]

            # Initialize collections
            self.individual_pdfs_collection = self.database.individual_pdf_extractions
            self.comparisons_collection = self.database.comparisons
            self.companies_collection = self.database.companies  # For sub-user company lookups

            # Test connection
            await self.client.admin.command("ping")
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
                [("comparison_id", 1), ("file_name", 1)], unique=True
            )

            # Index on extraction_timestamp (for sorting)
            await self.individual_pdfs_collection.create_index(
                "extraction_timestamp", expireAfterSeconds=2592000  # 30 days TTL
            )

            # ===== COMPARISONS COLLECTION INDEXES =====

            # Unique index on comparison_id
            await self.comparisons_collection.create_index("comparison_id", unique=True)

            # Index on created_at (for sorting)
            await self.comparisons_collection.create_index(
                "created_at", expireAfterSeconds=2592000  # 30 days TTL
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
        extracted_data: Dict[str, Any],
    ) -> str:
        """
        Save individual PDF extraction data to MongoDB with raw PDF binary.

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
                "policy_type": extracted_data.get("policy_type", "Unknown"),
                "premium_amount": extracted_data.get("premium_amount"),
                "rate": extracted_data.get("rate"),
                "score": extracted_data.get("score"),
                # Complete extracted data
                "extracted_data": extracted_data,
                # Metadata
                "extraction_timestamp": datetime.now(),
                "extraction_status": "success",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
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
        self, comparison_id: str, file_name: str
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
            document = await self.individual_pdfs_collection.find_one(
                {"comparison_id": comparison_id, "file_name": file_name}
            )

            if document:
                document["id"] = str(document["_id"])
                del document["_id"]
                return document

            return None

        except Exception as e:
            logger.error(f"❌ Error getting PDF by filename: {e}")
            raise

    async def get_pdf_binary_by_filename(
        self, comparison_id: str, file_name: str
    ) -> Optional[bytes]:
        """
        Get raw PDF binary data by filename.

        Args:
            comparison_id: Comparison ID
            file_name: File name

        Returns:
            Raw PDF binary data or None
        """
        try:
            document = await self.individual_pdfs_collection.find_one(
                {"comparison_id": comparison_id, "file_name": file_name},
                {
                    "pdf_binary": 1,
                    "original_filename": 1,
                },  # Only fetch binary and filename
            )

            if document and document.get("pdf_binary"):
                # pdf_binary is stored as Binary type, convert to bytes
                pdf_binary = document["pdf_binary"]
                if isinstance(pdf_binary, Binary):
                    # Binary type from bson, convert to bytes
                    return bytes(pdf_binary)
                elif isinstance(pdf_binary, bytes):
                    return pdf_binary
                else:
                    # Convert to bytes if needed
                    return bytes(pdf_binary)

            return None

        except Exception as e:
            logger.error(f"❌ Error getting PDF binary: {e}")
            raise

    async def get_all_pdfs_for_comparison(
        self, comparison_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all individual PDFs for a specific comparison.

        Args:
            comparison_id: Comparison ID

        Returns:
            List of PDF extraction documents
        """
        try:
            cursor = self.individual_pdfs_collection.find(
                {"comparison_id": comparison_id}
            ).sort("extraction_timestamp", 1)

            documents = []
            async for document in cursor:
                document["id"] = str(document["_id"])
                del document["_id"]
                documents.append(document)

            logger.info(
                f"📁 Retrieved {len(documents)} PDFs for comparison: {comparison_id}"
            )
            return documents

        except Exception as e:
            logger.error(f"❌ Error getting PDFs for comparison: {e}")
            raise

    async def get_pdfs_by_company(
        self, company_name: str, limit: int = 10
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
            cursor = (
                self.individual_pdfs_collection.find(
                    {"company_name": {"$regex": company_name, "$options": "i"}}
                )
                .sort("extraction_timestamp", -1)
                .limit(limit)
            )

            documents = []
            async for document in cursor:
                document["id"] = str(document["_id"])
                del document["_id"]
                documents.append(document)

            return documents

        except Exception as e:
            logger.error(f"❌ Error getting PDFs by company: {e}")
            raise

    async def delete_individual_pdf(self, comparison_id: str, file_name: str) -> bool:
        """
        Delete an individual PDF extraction.

        Args:
            comparison_id: Comparison ID
            file_name: File name

        Returns:
            True if deleted, False otherwise
        """
        try:
            result = await self.individual_pdfs_collection.delete_one(
                {"comparison_id": comparison_id, "file_name": file_name}
            )

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
        self, comparison_id: str, comparison_data: Dict[str, Any], pdf_count: int,
        user_id: Optional[str] = None, company_id: Optional[str] = None
    ) -> str:
        """
        Save final comparison result (combining all PDFs).
        Saves ALL data from AI ranking step - EVERYTHING A to Z.

        Args:
            comparison_id: Unique comparison ID
            comparison_data: Complete comparison result (ALL data from compare-quotes endpoint)
            pdf_count: Number of PDFs compared
            user_id: User ID who created the comparison (for access control)
            company_id: Company ID for sharing comparisons with sub-accounts

        Returns:
            MongoDB document ID
        """
        try:
            # Log what we're saving
            logger.info(f"💾 Saving COMPLETE comparison data to MongoDB")
            logger.info(f"   Comparison ID: {comparison_id}")
            logger.info(f"   Total PDFs: {pdf_count}")
            logger.info(f"   Data keys: {list(comparison_data.keys())}")

            # Log extracted_quotes details BEFORE saving
            extracted_quotes = comparison_data.get("extracted_quotes", [])
            if extracted_quotes:
                logger.info(f"📋 Extracted quotes count: {len(extracted_quotes)}")
                for idx, quote in enumerate(extracted_quotes[:2]):  # Log first 2 quotes
                    if isinstance(quote, dict):
                        logger.info(
                            f"   Quote {idx + 1}: {quote.get('company_name', 'Unknown')}"
                        )
                        logger.info(
                            f"      - Has _extended_data: {bool(quote.get('_extended_data'))}"
                        )
                        logger.info(
                            f"      - Has _analysis_details: {bool(quote.get('_analysis_details'))}"
                        )
                        logger.info(
                            f"      - Has _calculation_log: {bool(quote.get('_calculation_log'))}"
                        )
                        logger.info(
                            f"      - Has _quote_fingerprint: {bool(quote.get('_quote_fingerprint'))}"
                        )
                        logger.info(
                            f"      - Has _quality_metrics: {bool(quote.get('_quality_metrics'))}"
                        )
                        # Log all keys in quote
                        quote_keys = list(quote.keys())
                        logger.info(
                            f"      - All keys ({len(quote_keys)}): {quote_keys[:10]}..."
                        )  # First 10 keys

            # Prepare document - SAVE EVERYTHING (use comparison_data directly to preserve ALL fields)
            # Instead of selectively copying fields, save the ENTIRE comparison_data as-is
            document = {
                "comparison_id": comparison_id,
                "status": comparison_data.get("status", "completed"),
                "total_pdfs": pdf_count,
                "total_quotes": comparison_data.get("total_quotes", pdf_count),
                # ===== COMPLETE COMPARISON DATA (ALL DATA FROM AI RANKING) =====
                # Save the ENTIRE response - this ensures NOTHING is lost
                "comparison_data": comparison_data,  # Save the ENTIRE response
                # ===== ALL SECTIONS FROM AI RANKING (for easy access) =====
                # Summary section (with ranking, analysis_summary, best_overall, etc.)
                "summary": comparison_data.get("summary", {}),
                # Key differences (with differences list, summary, recommendation)
                "key_differences": comparison_data.get("key_differences", {}),
                # Side-by-side (with providers list, comparison_matrix with ALL data)
                "side_by_side": comparison_data.get("side_by_side", {}),
                # Data table (with columns and rows - ALL provider data)
                "data_table": comparison_data.get("data_table", {}),
                # Analytics (with charts, statistics, insights)
                "analytics": comparison_data.get("analytics", {}),
                # Provider cards (all provider details)
                "provider_cards": comparison_data.get("provider_cards", []),
                # Extracted quotes (full quote data for each provider - WITH ALL NESTED DATA)
                "extracted_quotes": extracted_quotes,  # Save with ALL nested fields
                # Charts (if separate from analytics)
                "charts": comparison_data.get(
                    "charts", comparison_data.get("analytics", {}).get("charts", [])
                ),
                # ===== METADATA =====
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "files_processed": comparison_data.get("files_processed", []),
                "processing_timestamp": comparison_data.get("processing_timestamp"),
                "processing_summary": comparison_data.get("processing_summary", {}),
                # Hakim Score enabled flag
                "hakim_score_enabled": comparison_data.get("hakim_score_enabled", True),
                # Additional fields that might be in comparison_data
                "job_id": comparison_data.get("job_id"),
                "usage": comparison_data.get("usage", {}),
                # ===== USER & COMPANY ACCESS CONTROL =====
                "user_id": user_id,  # User who created the comparison
                "company_id": company_id,  # Company ID for sharing with sub-accounts
            }

            # Log data sizes for debugging
            import json

            try:
                summary_size = len(json.dumps(document.get("summary", {}), default=str))
                side_by_side_size = len(
                    json.dumps(document.get("side_by_side", {}), default=str)
                )
                data_table_size = len(
                    json.dumps(document.get("data_table", {}), default=str)
                )
                analytics_size = len(
                    json.dumps(document.get("analytics", {}), default=str)
                )
                extracted_quotes_size = len(
                    json.dumps(document.get("extracted_quotes", []), default=str)
                )
                total_size = len(json.dumps(document, default=str))

                logger.info(f"📊 Data sizes:")
                logger.info(f"   Summary: {summary_size:,} bytes")
                logger.info(f"   Side-by-side: {side_by_side_size:,} bytes")
                logger.info(f"   Data table: {data_table_size:,} bytes")
                logger.info(f"   Analytics: {analytics_size:,} bytes")
                logger.info(f"   Extracted quotes: {extracted_quotes_size:,} bytes")
                logger.info(f"   Total document: {total_size:,} bytes")

                # Check if side_by_side has comparison_matrix with all data
                comparison_matrix = document.get("side_by_side", {}).get(
                    "comparison_matrix", {}
                )
                if comparison_matrix:
                    logger.info(
                        f"   Comparison matrix keys: {list(comparison_matrix.keys())}"
                    )
                    for key, value in comparison_matrix.items():
                        if isinstance(value, list):
                            logger.info(f"      {key}: {len(value)} items")
                        else:
                            logger.info(f"      {key}: {type(value).__name__}")

                # Check extracted_quotes structure
                if extracted_quotes:
                    logger.info(f"   Extracted quotes structure:")
                    logger.info(f"      - Count: {len(extracted_quotes)}")
                    if len(extracted_quotes) > 0:
                        first_quote = extracted_quotes[0]
                        if isinstance(first_quote, dict):
                            first_quote_keys = list(first_quote.keys())
                            logger.info(
                                f"      - First quote keys ({len(first_quote_keys)}): {first_quote_keys}"
                            )
                            # Check for nested data
                            if "_extended_data" in first_quote:
                                ext_data = first_quote["_extended_data"]
                                if isinstance(ext_data, dict):
                                    ext_keys = list(ext_data.keys())
                                    logger.info(
                                        f"      - _extended_data keys ({len(ext_keys)}): {ext_keys}"
                                    )
                            if "_analysis_details" in first_quote:
                                analysis_data = first_quote["_analysis_details"]
                                if isinstance(analysis_data, dict):
                                    analysis_keys = list(analysis_data.keys())
                                    logger.info(
                                        f"      - _analysis_details keys ({len(analysis_keys)}): {analysis_keys}"
                                    )
            except Exception as e:
                logger.warning(f"⚠️  Could not calculate data sizes: {e}")
                import traceback

                logger.warning(traceback.format_exc())

            # Insert or update
            result = await self.comparisons_collection.update_one(
                {"comparison_id": comparison_id}, {"$set": document}, upsert=True
            )

            if result.upserted_id:
                logger.info(f"✅ Saved new comparison: {comparison_id}")
                logger.info(f"   MongoDB ID: {result.upserted_id}")

                # Verify what was saved - check ALL fields including nested data
                saved_doc = await self.comparisons_collection.find_one(
                    {"_id": result.upserted_id}
                )
                if saved_doc:
                    logger.info(f"✅ Verification - Saved document has:")
                    logger.info(
                        f"   - comparison_data: {bool(saved_doc.get('comparison_data'))}"
                    )
                    logger.info(f"   - summary: {bool(saved_doc.get('summary'))}")
                    logger.info(
                        f"   - side_by_side: {bool(saved_doc.get('side_by_side'))}"
                    )
                    logger.info(f"   - data_table: {bool(saved_doc.get('data_table'))}")
                    logger.info(f"   - analytics: {bool(saved_doc.get('analytics'))}")
                    logger.info(
                        f"   - extracted_quotes: {bool(saved_doc.get('extracted_quotes'))}"
                    )

                    # Check data_table rows
                    data_table = saved_doc.get("data_table", {})
                    rows = data_table.get("rows", [])
                    if rows:
                        logger.info(f"   - data_table rows: {len(rows)}")
                        if len(rows) > 0:
                            first_row_keys = (
                                list(rows[0].keys())
                                if isinstance(rows[0], dict)
                                else []
                            )
                            logger.info(
                                f"   - First row keys ({len(first_row_keys)}): {first_row_keys[:15]}..."
                            )  # First 15 keys

                    # Check side_by_side providers
                    side_by_side = saved_doc.get("side_by_side", {})
                    providers = side_by_side.get("providers", [])
                    if providers:
                        logger.info(f"   - side_by_side providers: {len(providers)}")
                        if len(providers) > 0:
                            first_provider_keys = (
                                list(providers[0].keys())
                                if isinstance(providers[0], dict)
                                else []
                            )
                            logger.info(
                                f"   - First provider keys ({len(first_provider_keys)}): {first_provider_keys[:15]}..."
                            )  # First 15 keys

                    # Check extracted_quotes - VERIFY ALL NESTED DATA IS SAVED
                    saved_extracted_quotes = saved_doc.get("extracted_quotes", [])
                    if saved_extracted_quotes:
                        logger.info(
                            f"   - extracted_quotes count: {len(saved_extracted_quotes)}"
                        )
                        if len(saved_extracted_quotes) > 0:
                            first_saved_quote = saved_extracted_quotes[0]
                            if isinstance(first_saved_quote, dict):
                                saved_quote_keys = list(first_saved_quote.keys())
                                logger.info(
                                    f"   - First saved quote keys ({len(saved_quote_keys)}): {saved_quote_keys}"
                                )
                                # Verify nested data is present
                                logger.info(
                                    f"   - Has _extended_data: {bool(first_saved_quote.get('_extended_data'))}"
                                )
                                logger.info(
                                    f"   - Has _analysis_details: {bool(first_saved_quote.get('_analysis_details'))}"
                                )
                                logger.info(
                                    f"   - Has _calculation_log: {bool(first_saved_quote.get('_calculation_log'))}"
                                )
                                logger.info(
                                    f"   - Has _quote_fingerprint: {bool(first_saved_quote.get('_quote_fingerprint'))}"
                                )
                                logger.info(
                                    f"   - Has _quality_metrics: {bool(first_saved_quote.get('_quality_metrics'))}"
                                )

                                # Check _extended_data structure
                                if first_saved_quote.get("_extended_data"):
                                    ext_data = first_saved_quote["_extended_data"]
                                    if isinstance(ext_data, dict):
                                        ext_keys = list(ext_data.keys())
                                        logger.info(
                                            f"   - _extended_data keys ({len(ext_keys)}): {ext_keys}"
                                        )

                return str(result.upserted_id)
            else:
                logger.info(f"✅ Updated comparison: {comparison_id}")
                # Get the existing document ID
                existing = await self.comparisons_collection.find_one(
                    {"comparison_id": comparison_id}
                )
                if existing:
                    logger.info(f"   MongoDB ID: {existing['_id']}")
                    # Verify what was saved
                    logger.info(f"✅ Verification - Updated document has:")
                    logger.info(
                        f"   - comparison_data: {bool(existing.get('comparison_data'))}"
                    )
                    logger.info(
                        f"   - data_table rows: {len(existing.get('data_table', {}).get('rows', []))}"
                    )
                    logger.info(
                        f"   - side_by_side providers: {len(existing.get('side_by_side', {}).get('providers', []))}"
                    )
                    logger.info(
                        f"   - extracted_quotes count: {len(existing.get('extracted_quotes', []))}"
                    )
                    # Check if extracted_quotes have nested data
                    saved_quotes = existing.get("extracted_quotes", [])
                    if saved_quotes and len(saved_quotes) > 0:
                        first_quote = saved_quotes[0]
                        if isinstance(first_quote, dict):
                            logger.info(
                                f"   - First quote has _extended_data: {bool(first_quote.get('_extended_data'))}"
                            )
                            logger.info(
                                f"   - First quote has _analysis_details: {bool(first_quote.get('_analysis_details'))}"
                            )
                return str(existing["_id"]) if existing else None

        except Exception as e:
            logger.error(f"❌ Error saving comparison result: {e}")
            import traceback

            logger.error(traceback.format_exc())
            raise

    async def get_comparison_result(
        self, comparison_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comparison result by ID.

        Args:
            comparison_id: Comparison ID

        Returns:
            Comparison data or None
        """
        try:
            document = await self.comparisons_collection.find_one(
                {"comparison_id": comparison_id}
            )

            if document:
                document["id"] = str(document["_id"])
                del document["_id"]
                return document

            return None

        except Exception as e:
            logger.error(f"❌ Error getting comparison result: {e}")
            raise

    async def get_recent_comparisons(
        self, limit: int = 10, skip: int = 0
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
            cursor = (
                self.comparisons_collection.find()
                .sort("created_at", -1)
                .skip(skip)
                .limit(limit)
            )

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

    async def get_comparisons_by_user(
        self, user_id: str, company_id: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get all comparisons accessible to a user.
        Returns comparisons created by the user OR shared via company_id.
        Returns FULL comparison_data for frontend to store in localStorage.

        For sub-users:
        - company_id is the Company document's _id (not the parent user's ID)
        - We need to look up the Company to find the parent user (createdBy field)
        - Then include comparisons created by the parent user

        Args:
            user_id: User ID
            company_id: Optional company ID (if user belongs to a company)
            limit: Maximum results (default 100)

        Returns:
            List of comparison documents with full comparison_data
        """
        try:
            # Build query: user's own comparisons OR company-shared comparisons
            query_conditions = [{"user_id": user_id}]
            
            # If company_id is provided, also include company-shared comparisons
            if company_id:
                # Include comparisons where company_id matches
                query_conditions.append({"company_id": company_id})
                
                # CRITICAL FIX: For sub-users, company_id is the Company document's _id
                # We need to look up the Company to find the parent user (createdBy)
                # Then include comparisons created by the parent user
                try:
                    if ObjectId.is_valid(company_id):
                        company = await self.companies_collection.find_one({"_id": ObjectId(company_id)})
                        if company and company.get("createdBy"):
                            parent_user_id = str(company["createdBy"])
                            logger.info(f"🔍 Found parent user {parent_user_id} for company {company_id}")
                            
                            # Include ALL comparisons created by the parent user
                            # (covers both old comparisons with company_id=null and new ones)
                            if parent_user_id != user_id:
                                query_conditions.append({"user_id": parent_user_id})
                except Exception as company_lookup_error:
                    logger.warning(f"⚠️  Could not look up company {company_id}: {company_lookup_error}")
            
            # Use $or if multiple conditions, otherwise use single condition
            if len(query_conditions) > 1:
                query = {"$or": query_conditions}
            else:
                query = query_conditions[0]
            
            logger.info(f"🔍 Query for comparisons: {query}")
            
            cursor = (
                self.comparisons_collection.find(query)
                .sort("created_at", -1)
                .limit(limit)
            )

            documents = []
            async for document in cursor:
                document["id"] = str(document["_id"])
                del document["_id"]
                # Return FULL comparison_data for frontend localStorage
                documents.append(document)

            logger.info(f"📊 Found {len(documents)} comparisons for user {user_id} (company: {company_id})")
            return documents

        except Exception as e:
            logger.error(f"❌ Error getting comparisons by user: {e}")
            raise

    async def delete_comparison_result(self, comparison_id: str) -> bool:
        """
        Delete a comparison result.

        Args:
            comparison_id: Comparison ID

        Returns:
            True if deleted, False otherwise
        """
        try:
            result = await self.comparisons_collection.delete_one(
                {"comparison_id": comparison_id}
            )

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
        self, comparison_id: str
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
                "pdf_count": len(individual_pdfs),
            }

            logger.info(
                f"📦 Retrieved complete comparison: {comparison_id} ({len(individual_pdfs)} PDFs)"
            )
            return result

        except Exception as e:
            logger.error(f"❌ Error getting complete comparison: {e}")
            raise

    async def delete_complete_comparison(self, comparison_id: str) -> Dict[str, int]:
        """
        Delete comparison result AND all associated individual PDFs.

        Args:
            comparison_id: Comparison ID

        Returns:
            Dict with deletion counts
        """
        try:
            # Delete all individual PDFs
            pdf_result = await self.individual_pdfs_collection.delete_many(
                {"comparison_id": comparison_id}
            )

            # Delete comparison result
            comp_result = await self.comparisons_collection.delete_one(
                {"comparison_id": comparison_id}
            )

            logger.info(
                f"🗑️  Deleted comparison {comparison_id}: "
                f"{pdf_result.deleted_count} PDFs, "
                f"{comp_result.deleted_count} comparison"
            )

            return {
                "pdfs_deleted": pdf_result.deleted_count,
                "comparison_deleted": comp_result.deleted_count,
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
                {"$group": {"_id": "$company_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ]

            top_companies = []
            async for doc in self.individual_pdfs_collection.aggregate(pipeline):
                top_companies.append({"company": doc["_id"], "pdf_count": doc["count"]})

            # Get recent activity
            recent_pdfs = await self.individual_pdfs_collection.count_documents(
                {
                    "extraction_timestamp": {
                        "$gte": datetime.now().replace(hour=0, minute=0, second=0)
                    }
                }
            )

            return {
                "total_pdfs_stored": total_pdfs,
                "total_comparisons": total_comparisons,
                "top_companies": top_companies,
                "pdfs_today": recent_pdfs,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"❌ Error getting statistics: {e}")
            return {"error": str(e)}


# Global instance
enhanced_mongodb_service = EnhancedMongoDBService()
