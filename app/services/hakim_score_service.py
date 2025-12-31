"""
Hakim Score Management Service
==============================
Service for managing Hakim scores in MongoDB.
Allows admin to adjust scores dynamically without code changes.
"""

import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import os
import re
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


# Score to Tier mapping (auto-assign tier based on score)
SCORE_TO_TIER_MAPPING = {
    (0.95, 1.0): "Premium",
    (0.90, 0.95): "Premium",
    (0.85, 0.90): "Strong",
    (0.80, 0.85): "Strong",
    (0.75, 0.80): "Solid",
    (0.70, 0.75): "Solid",
    (0.65, 0.70): "Baseline",
    (0.60, 0.65): "Baseline",
    (0.50, 0.60): "Challenged",
    (0.0, 0.50): "Challenged",
    (0.0, 0.0): "Disabled"  # Special case for zero
}


def _get_tier_from_score(score: float) -> str:
    """
    Automatically determine tier based on score.
    
    Args:
        score: Score value (0.0 to 1.0)
        
    Returns:
        Tier name
    """
    if score == 0.0:
        return "Disabled"
    
    for (min_score, max_score), tier in SCORE_TO_TIER_MAPPING.items():
        if min_score <= score <= max_score:
            return tier
    
    # Default fallback
    if score >= 0.9:
        return "Premium"
    elif score >= 0.85:
        return "Strong"
    elif score >= 0.75:
        return "Solid"
    elif score >= 0.65:
        return "Baseline"
    else:
        return "Challenged"


class HakimScoreService:
    """
    Service for managing Hakim scores in MongoDB.
    Provides CRUD operations for admin to manage company scores.
    """
    
    def __init__(self):
        self.client = None
        self.database = None
        self.collection = None
        
    async def connect(self):
        """Connect to MongoDB and initialize collection"""
        try:
            mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
            database_name = os.getenv("MONGODB_DATABASE", "hakemAI")
            
            logger.info(f"🔌 Connecting Hakim Score Service to MongoDB: {mongodb_url}")
            
            self.client = AsyncIOMotorClient(mongodb_url)
            self.database = self.client[database_name]
            self.collection = self.database.hakim_scores
            
            # Test connection
            await self.client.admin.command('ping')
            logger.info("✅ Hakim Score Service connected to MongoDB")
            
            # Create indexes
            await self._create_indexes()
            
        except ConnectionFailure as e:
            logger.error(f"❌ Failed to connect Hakim Score Service: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting Hakim Score Service: {e}")
            raise
    
    async def _create_indexes(self):
        """Create database indexes for better performance"""
        try:
            # Unique index on company_name (case-insensitive)
            await self.collection.create_index(
                [("company_name", 1)],
                unique=True,
                collation={"locale": "en", "strength": 2}  # Case-insensitive
            )
            
            # Index on tier for filtering
            await self.collection.create_index("tier")
            
            # Index on rank for sorting
            await self.collection.create_index("rank")
            
            # Index on aliases for faster alias searches
            await self.collection.create_index("aliases")
            
            logger.info("✅ Hakim Score indexes created successfully")
            
        except Exception as e:
            logger.error(f"⚠️  Error creating Hakim Score indexes: {e}")
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("🔌 Hakim Score Service disconnected from MongoDB")
    
    def _normalize_company_name(self, name: str) -> str:
        """
        Normalize company name for better matching.
        Removes common words, handles abbreviations, normalizes spacing.
        Preserves Arabic/Urdu characters for proper matching.
        """
        if not name:
            return ""
        
        # Convert to lowercase (works for English, preserves Arabic/Urdu)
        normalized = name.lower().strip()
        
        # Remove common words (English only - Arabic names won't match these)
        common_words = [
            "the", "company", "co", "co.", "ltd", "ltd.", "limited", 
            "inc", "inc.", "incorporated", "llc", "group", "insurance",
            "assurance", "cooperative", "co-operative", "takaful"
        ]
        
        for word in common_words:
            # Remove word with surrounding spaces/punctuation (case-insensitive)
            normalized = re.sub(rf'\b{re.escape(word)}\b', '', normalized, flags=re.IGNORECASE)
        
        # Remove punctuation but preserve Unicode letters (Arabic/Urdu/English)
        # \w in Python matches Unicode word characters including Arabic/Urdu
        normalized = re.sub(r'[^\w\s]', '', normalized, flags=re.UNICODE)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between two company names.
        Returns a value between 0.0 and 1.0.
        """
        norm1 = self._normalize_company_name(name1)
        norm2 = self._normalize_company_name(name2)
        
        if not norm1 or not norm2:
            return 0.0
        
        # Exact match after normalization
        if norm1 == norm2:
            return 1.0
        
        # Check if one contains the other
        if norm1 in norm2 or norm2 in norm1:
            return 0.9
        
        # Use SequenceMatcher for fuzzy matching
        similarity = SequenceMatcher(None, norm1, norm2).ratio()
        
        return similarity
    
    def _extract_keywords(self, name: str) -> set:
        """
        Extract key keywords from company name for matching.
        """
        normalized = self._normalize_company_name(name)
        # Split into words and filter out very short words
        words = [w for w in normalized.split() if len(w) > 2]
        return set(words)
    
    def _format_document_for_response(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format MongoDB document for JSON response.
        Converts ObjectId to string and datetime to ISO format.
        """
        if not document:
            return {}
        
        formatted = {
            "id": str(document["_id"]),
            "company_name": document.get("company_name", ""),
            "score": document.get("score", 0.0),
            "score_display": round(document.get("score", 0.0) * 100, 2),
            "tier": document.get("tier", "Standard"),
            "rank": document.get("rank", 999),
            "aliases": document.get("aliases", []),
            "is_zero": document.get("score", 0.0) == 0.0
        }
        
        # Convert datetime objects to ISO format strings
        if "created_at" in document and document["created_at"]:
            if isinstance(document["created_at"], datetime):
                formatted["created_at"] = document["created_at"].isoformat()
            else:
                formatted["created_at"] = str(document["created_at"])
        else:
            formatted["created_at"] = None
        
        if "updated_at" in document and document["updated_at"]:
            if isinstance(document["updated_at"], datetime):
                formatted["updated_at"] = document["updated_at"].isoformat()
            else:
                formatted["updated_at"] = str(document["updated_at"])
        else:
            formatted["updated_at"] = None
        
        return formatted
    
    async def get_hakim_score(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        Get Hakim score for a company with intelligent matching.
        Handles variations like "GIG" matching "Gulf Insurance Group (GIG)".
        
        Args:
            company_name: Company name (case-insensitive, fuzzy matching)
            
        Returns:
            Hakim score data or None if not found
        """
        try:
            # Step 1: Try exact match first (case-insensitive, handles Arabic/Urdu)
            document = await self.collection.find_one(
                {"company_name": {"$regex": f"^{re.escape(company_name)}$", "$options": "i"}}
            )
            
            if document:
                logger.debug(f"✅ Exact match found for: {company_name}")
                return self._format_document_for_response(document)
            
            # Step 2: Try matching against aliases and fuzzy matching
            # Get all documents from collection directly (not formatted) to access aliases properly
            cursor = self.collection.find({})
            all_documents_raw = []
            async for doc in cursor:
                all_documents_raw.append(doc)
            
            company_name_lower = company_name.lower().strip()
            company_name_normalized = self._normalize_company_name(company_name)
            input_keywords = self._extract_keywords(company_name)
            
            best_match = None
            best_similarity = 0.0
            
            for doc in all_documents_raw:
                doc_company_name = doc.get("company_name", "")
                doc_aliases = doc.get("aliases", [])
                
                # Check exact match with company name (case-insensitive, handles Arabic/Urdu)
                if company_name_lower == doc_company_name.lower().strip():
                    logger.info(f"✅ Exact match found: '{company_name}' → '{doc_company_name}'")
                    return self._format_document_for_response(doc)
                
                # Check exact match with aliases (case-insensitive) - handles Arabic/Urdu names
                for alias in doc_aliases:
                    if company_name_lower == alias.lower().strip():
                        logger.info(f"✅ Alias match found: '{company_name}' → '{doc_company_name}' (via alias: '{alias}')")
                        return self._format_document_for_response(doc)
                
                # Calculate similarity with company name
                similarity = self._calculate_similarity(company_name, doc_company_name)
                
                # Calculate similarity with aliases (including Arabic/Urdu)
                for alias in doc_aliases:
                    alias_similarity = self._calculate_similarity(company_name, alias)
                    if alias_similarity > similarity:
                        similarity = alias_similarity
                
                # Check keyword overlap
                doc_keywords = self._extract_keywords(doc_company_name)
                keyword_overlap = len(input_keywords & doc_keywords) / max(len(input_keywords), len(doc_keywords), 1)
                
                # Combined score: similarity + keyword overlap
                combined_score = (similarity * 0.7) + (keyword_overlap * 0.3)
                
                # Check for abbreviation match (e.g., "GIG" in "Gulf Insurance Group")
                if len(company_name) <= 5 and company_name.isupper():
                    # Likely an abbreviation
                    doc_name_upper = doc_company_name.upper()
                    if company_name in doc_name_upper or doc_name_upper.startswith(company_name):
                        combined_score = max(combined_score, 0.85)
                    
                    # Check aliases for abbreviation match
                    for alias in doc_aliases:
                        if company_name in alias.upper():
                            combined_score = max(combined_score, 0.85)
                
                # Check if input is abbreviation of company name
                if len(company_name) <= 5:
                    # Extract first letters of words
                    doc_words = doc_company_name.split()
                    doc_abbrev = "".join([w[0].upper() for w in doc_words if w and w[0].isalpha()])
                    if company_name.upper() == doc_abbrev:
                        combined_score = max(combined_score, 0.9)
                
                if combined_score > best_similarity:
                    best_similarity = combined_score
                    best_match = doc
            
            # Use best match if similarity is high enough (threshold: 0.6)
            if best_match and best_similarity >= 0.6:
                logger.info(f"✅ Intelligent match found: '{company_name}' → '{best_match['company_name']}' (similarity: {best_similarity:.2f})")
                return self._format_document_for_response(best_match)
            
            logger.debug(f"⚠️ No match found for: {company_name} (best similarity: {best_similarity:.2f})")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting Hakim score for {company_name}: {e}")
            return None
    
    async def get_all_hakim_scores(
        self,
        sort_by: str = "company_name",
        sort_order: int = 1,
        include_zero_scores: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get all Hakim scores with flexible sorting and filtering.
        Optimized for admin scoring page - returns simple format for table display.
        
        Args:
            sort_by: Field to sort by (rank, company_name, score, tier, updated_at) - Default: company_name
            sort_order: 1 for ascending, -1 for descending
            include_zero_scores: Whether to include companies with score = 0.0
            
        Returns:
            List of all Hakim score documents, formatted for frontend table
            Each document includes: id, company_name, score, score_display, tier, rank, is_zero
        """
        try:
            # Build query filter
            query = {}
            if not include_zero_scores:
                query["score"] = {"$gt": 0.0}
            
            # Validate sort field
            valid_sort_fields = ["rank", "company_name", "score", "tier", "updated_at"]
            if sort_by not in valid_sort_fields:
                sort_by = "company_name"  # Default to company_name for alphabetical listing
            
            # Execute query with sorting
            cursor = self.collection.find(query).sort(sort_by, sort_order)
            
            documents = []
            async for document in cursor:
                # Format for frontend using helper method
                formatted_doc = self._format_document_for_response(document)
                documents.append(formatted_doc)
            
            logger.info(f"📊 Retrieved {len(documents)} Hakim scores (sorted by {sort_by}, order: {sort_order})")
            return documents
            
        except Exception as e:
            logger.error(f"❌ Error getting all Hakim scores: {e}")
            return []
    
    async def create_or_update_hakim_score(
        self,
        company_name: str,
        score: float,
        tier: str,
        rank: int,
        aliases: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create or update Hakim score for a company.
        Supports score = 0.0 (explicitly allowed for admin to disable companies).
        
        Args:
            company_name: Company name
            score: Score value (0.0 to 1.0, will be multiplied by 100 for display)
                   Score of 0.0 is allowed to disable/zero out a company
            tier: Tier name (Premium, Strong, Solid, Baseline, Challenged, Standard)
            rank: Rank number (1 = highest)
            aliases: Optional list of company name aliases/variations
            
        Returns:
            Created/updated document
        """
        try:
            # Validate score range (0.0 is explicitly allowed)
            if score < 0.0 or score > 1.0:
                raise ValueError(f"Score must be between 0.0 and 1.0 (inclusive), got {score}")
            
            # If score is 0.0, set tier to a default value if not provided
            if score == 0.0 and not tier:
                tier = "Disabled"
            
            # Prepare document
            document = {
                "company_name": company_name,
                "score": score,
                "tier": tier,
                "rank": rank,
                "aliases": aliases or [],
                "updated_at": datetime.utcnow(),
                "created_at": datetime.utcnow()
            }
            
            # Try to find existing document (case-insensitive)
            existing = await self.collection.find_one(
                {"company_name": {"$regex": f"^{company_name}$", "$options": "i"}}
            )
            
            if existing:
                # Update existing
                document["created_at"] = existing.get("created_at", datetime.utcnow())
                result = await self.collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": document}
                )
                
                if result.modified_count > 0:
                    logger.info(f"✅ Updated Hakim score for {company_name}")
                    # Return updated document
                    updated = await self.collection.find_one({"_id": existing["_id"]})
                    return self._format_document_for_response(updated)
            else:
                # Create new
                result = await self.collection.insert_one(document)
                
                if result.inserted_id:
                    logger.info(f"✅ Created Hakim score for {company_name}")
                    # Return created document
                    created = await self.collection.find_one({"_id": result.inserted_id})
                    return self._format_document_for_response(created)
            
            raise Exception("Failed to create or update Hakim score")
            
        except DuplicateKeyError:
            logger.error(f"❌ Duplicate company name: {company_name}")
            raise ValueError(f"Company already exists: {company_name}")
        except Exception as e:
            logger.error(f"❌ Error creating/updating Hakim score: {e}")
            raise
    
    async def update_score_only(
        self,
        company_name: str,
        score: float
    ) -> Dict[str, Any]:
        """
        Update ONLY the Hakim score for a company (simplified for scoring page).
        Automatically determines tier based on score.
        Keeps existing rank and aliases.
        
        Args:
            company_name: Company name
            score: New score value (0.0 to 1.0, 0.0 allowed to disable)
            
        Returns:
            Updated document
        """
        try:
            # Validate score range
            if score < 0.0 or score > 1.0:
                raise ValueError(f"Score must be between 0.0 and 1.0 (inclusive), got {score}")
            
            # Find existing document (case-insensitive)
            existing = await self.collection.find_one(
                {"company_name": {"$regex": f"^{re.escape(company_name)}$", "$options": "i"}}
            )
            
            if not existing:
                raise ValueError(f"Company not found: {company_name}")
            
            # Auto-determine tier from score
            auto_tier = _get_tier_from_score(score)
            
            # Build update document
            update_doc = {
                "score": score,
                "tier": auto_tier,
                "updated_at": datetime.utcnow()
            }
            
            # Update
            result = await self.collection.update_one(
                {"_id": existing["_id"]},
                {"$set": update_doc}
            )
            
            if result.modified_count > 0:
                logger.info(f"✅ Updated score for {company_name}: {score} (tier: {auto_tier})")
                # Return updated document
                updated = await self.collection.find_one({"_id": existing["_id"]})
                return self._format_document_for_response(updated)
            else:
                raise Exception("No changes made")
            
        except Exception as e:
            logger.error(f"❌ Error updating score: {e}")
            raise
    
    async def update_hakim_score(
        self,
        company_name: str,
        score: Optional[float] = None,
        tier: Optional[str] = None,
        rank: Optional[int] = None,
        aliases: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Update specific fields of a Hakim score (full update).
        Supports setting score to 0.0 to disable/zero out a company.
        
        Args:
            company_name: Company name
            score: Optional new score value (0.0 to 1.0, 0.0 is allowed)
            tier: Optional new tier (auto-determined from score if not provided)
            rank: Optional new rank
            aliases: Optional new aliases list
            
        Returns:
            Updated document
        """
        try:
            # Find existing document (case-insensitive)
            existing = await self.collection.find_one(
                {"company_name": {"$regex": f"^{re.escape(company_name)}$", "$options": "i"}}
            )
            
            if not existing:
                raise ValueError(f"Company not found: {company_name}")
            
            # Build update document
            update_doc = {"updated_at": datetime.utcnow()}
            
            if score is not None:
                # Validate score range (0.0 is explicitly allowed)
                if score < 0.0 or score > 1.0:
                    raise ValueError(f"Score must be between 0.0 and 1.0 (inclusive), got {score}")
                update_doc["score"] = score
                
                # Auto-determine tier from score if tier not provided
                if tier is None:
                    update_doc["tier"] = _get_tier_from_score(score)
                else:
                    update_doc["tier"] = tier
            
            if tier is not None and score is None:
                # Only update tier if score not being updated
                update_doc["tier"] = tier
            
            if rank is not None:
                update_doc["rank"] = rank
            
            if aliases is not None:
                update_doc["aliases"] = aliases
            
            # Update
            result = await self.collection.update_one(
                {"_id": existing["_id"]},
                {"$set": update_doc}
            )
            
            if result.modified_count > 0:
                logger.info(f"✅ Updated Hakim score for {company_name}")
                # Return updated document
                updated = await self.collection.find_one({"_id": existing["_id"]})
                return self._format_document_for_response(updated)
            else:
                raise Exception("No changes made")
            
        except Exception as e:
            logger.error(f"❌ Error updating Hakim score: {e}")
            raise
    
    async def delete_hakim_score(self, company_name: str) -> bool:
        """
        Delete Hakim score for a company.
        
        Args:
            company_name: Company name
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            result = await self.collection.delete_one(
                {"company_name": {"$regex": f"^{company_name}$", "$options": "i"}}
            )
            
            if result.deleted_count > 0:
                logger.info(f"🗑️  Deleted Hakim score for {company_name}")
                return True
            else:
                logger.warning(f"⚠️  Hakim score not found: {company_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error deleting Hakim score: {e}")
            raise
    
    async def bulk_create_or_update(
        self,
        scores: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Bulk create or update multiple Hakim scores.
        Production-optimized with transaction support and error handling.
        
        Args:
            scores: List of score dictionaries with keys: company_name, score, tier, rank, aliases (optional)
                   Score can be 0.0 to disable/zero out companies
            
        Returns:
            Dict with counts: created, updated, failed, and detailed results
        """
        created = 0
        updated = 0
        failed = 0
        results = []
        
        for score_data in scores:
            try:
                company_name = score_data.get("company_name", "").strip()
                if not company_name:
                    raise ValueError("Company name is required")
                
                score = score_data.get("score", 0.0)
                tier = score_data.get("tier", "Standard")
                rank = score_data.get("rank", 999)
                aliases = score_data.get("aliases", [])
                
                # Validate score (0.0 is allowed)
                if score < 0.0 or score > 1.0:
                    raise ValueError(f"Score must be between 0.0 and 1.0, got {score}")
                
                # If score is 0.0, set tier to Disabled if not provided
                if score == 0.0 and not tier:
                    tier = "Disabled"
                
                # Check if exists (case-insensitive)
                existing = await self.collection.find_one(
                    {"company_name": {"$regex": f"^{re.escape(company_name)}$", "$options": "i"}}
                )
                
                is_update = existing is not None
                
                # Create or update
                result = await self.create_or_update_hakim_score(
                    company_name=company_name,
                    score=score,
                    tier=tier,
                    rank=rank,
                    aliases=aliases
                )
                
                if is_update:
                    updated += 1
                else:
                    created += 1
                
                results.append({
                    "company_name": company_name,
                    "status": "updated" if is_update else "created",
                    "success": True
                })
                
            except Exception as e:
                failed += 1
                error_msg = str(e)
                logger.error(f"❌ Failed to process {score_data.get('company_name', 'unknown')}: {error_msg}")
                results.append({
                    "company_name": score_data.get("company_name", "unknown"),
                    "status": "failed",
                    "success": False,
                    "error": error_msg
                })
        
        logger.info(f"📊 Bulk operation complete: {created} created, {updated} updated, {failed} failed")
        
        return {
            "created": created,
            "updated": updated,
            "failed": failed,
            "total": len(scores),
            "results": results
        }
    
    async def bulk_update_scores(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Bulk update only scores (optimized for scoring page).
        Automatically determines tier from score.
        More efficient than full create/update when only scores change.
        
        Args:
            updates: List of dicts with company_name and score
                   Format: [{"company_name": "GIG", "score": 0.88}, ...]
            
        Returns:
            Dict with operation results
        """
        updated = 0
        failed = 0
        results = []
        
        for update_data in updates:
            try:
                company_name = update_data.get("company_name", "").strip()
                if not company_name:
                    raise ValueError("Company name is required")
                
                score = update_data.get("score")
                
                if score is None:
                    raise ValueError("Score is required")
                
                # Validate score (0.0 is allowed)
                if not isinstance(score, (int, float)) or score < 0.0 or score > 1.0:
                    raise ValueError(f"Score must be a number between 0.0 and 1.0, got {score}")
                
                # Use simplified update method (auto-determines tier)
                result = await self.update_score_only(
                    company_name=company_name,
                    score=float(score)
                )
                
                updated += 1
                results.append({
                    "company_name": company_name,
                    "status": "updated",
                    "success": True,
                    "new_score": float(score),
                    "new_tier": result.get("tier")
                })
                
            except ValueError as e:
                # Company not found or validation error
                failed += 1
                results.append({
                    "company_name": update_data.get("company_name", "unknown"),
                    "status": "failed",
                    "success": False,
                    "error": str(e)
                })
            except Exception as e:
                failed += 1
                logger.error(f"❌ Failed to update {update_data.get('company_name', 'unknown')}: {e}")
                results.append({
                    "company_name": update_data.get("company_name", "unknown"),
                    "status": "failed",
                    "success": False,
                    "error": str(e)
                })
        
        return {
            "updated": updated,
            "failed": failed,
            "total": len(updates),
            "results": results
        }
    
    async def search_companies(self, query: str) -> List[Dict[str, Any]]:
        """
        Search companies by name or alias.
        
        Args:
            query: Search query
            
        Returns:
            List of matching companies
        """
        try:
            # Search in company_name and aliases
            cursor = self.collection.find({
                "$or": [
                    {"company_name": {"$regex": query, "$options": "i"}},
                    {"aliases": {"$regex": query, "$options": "i"}}
                ]
            }).sort("rank", 1)
            
            documents = []
            async for document in cursor:
                formatted_doc = self._format_document_for_response(document)
                documents.append(formatted_doc)
            
            return documents
            
        except Exception as e:
            logger.error(f"❌ Error searching companies: {e}")
            return []


# Global instance
hakim_score_service = HakimScoreService()

 