"""
ENHANCED API ROUTES v3.2 - Individual PDF Storage
==================================================
✅ NEW FEATURES:
- Stores EACH PDF extraction individually in MongoDB
- Links all PDFs with same comparison_id
- Stores final comparison result separately
- No data mixing between PDFs
- Easy retrieval by file, company, or comparison

✅ INTEGRATIONS:
- Enhanced MongoDB Service v2.0
- AI Parser v6.0
- AI Ranker v3.0 (with full lists support)
- Individual PDF tracking

Version: 3.2 - Production Ready with Individual PDF Storage
Last Updated: 2025-10-28
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, Header, Query
from fastapi.responses import JSONResponse, Response
from typing import List, Dict, Any, Optional
import logging
import os
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
import asyncio
from io import BytesIO

from app.core.config import settings
from app.services.pdf_extractor import pdf_extractor
from app.services.ai_parser import ai_parser
from app.services.ai_ranker import quote_ranker
from app.models.quote_model import ExtractedQuoteData

# Import ENHANCED MongoDB service
from app.services.mongodb_service_enhanced import enhanced_mongodb_service

# Import Progress Tracker
from app.services.progress_tracker import progress_tracker

# Import User Subscription Service
from app.services.user_subscription_service import user_subscription_service
from app.services.user_documents_service import user_documents_service

# Import PDF Generator Service
from app.services.pdf_generator_service import pdf_generator_service

# Import Activity Logs Service
from app.services.activity_logs_service import activity_logs_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Insurance Quotes"])


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def _generate_comparison_id() -> str:
    """Generate unique comparison ID."""
    timestamp = int(datetime.now().timestamp())
    unique_id = uuid.uuid4().hex[:12]
    return f"cmp_{timestamp}_{unique_id}"


async def _save_uploaded_file(file: UploadFile, upload_dir: str) -> Dict[str, Any]:
    """
    Save uploaded file to disk and return file data including binary content.

    Returns:
        Dict with file information including binary content for MongoDB storage
    """
    try:
        # Generate unique filename
        file_extension = Path(file.filename).suffix
        unique_filename = f"{uuid.uuid4().hex}{file_extension}"
        file_path = os.path.join(upload_dir, unique_filename)

        # Read file content as binary
        # Reset file pointer to beginning (in case it was read before)
        await file.seek(0)
        content = await file.read()

        # Save file to disk
        with open(file_path, "wb") as f:
            f.write(content)

        file_size = len(content)

        logger.info(
            f"💾 Saved file: {file.filename} → {unique_filename} ({file_size} bytes)"
        )

        return {
            "file_name": unique_filename,
            "original_filename": file.filename,
            "file_path": file_path,
            "file_size": file_size,
            "file_content": content,  # Binary content for MongoDB storage
            "file_extension": file_extension,
        }

    except Exception as e:
        logger.error(f"❌ Error saving file: {e}")
        raise


# ============================================================================
# PROGRESS ENDPOINT
# ============================================================================


@router.get("/progress/{job_id}")
async def get_progress(job_id: str):
    """
    Get current progress for a job.

    Args:
        job_id: Job identifier

    Returns:
        Progress data with percentage, current step, and details
    """
    progress = progress_tracker.get_progress(job_id)

    if not progress:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    return JSONResponse(content=progress)


# ============================================================================
# MAIN COMPARISON ENDPOINT - ENHANCED WITH INDIVIDUAL PDF STORAGE
# ============================================================================


@router.post("/compare-quotes")
async def compare_insurance_quotes(
    files: List[UploadFile] = File(...),
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    # Get user ID from header or query parameter
    user_id = user_id or user_id_query

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="User ID is required. Please provide X-User-Id header or userId query parameter.",
        )

    # Validation
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="No files uploaded")

    if len(files) > 10:
        raise HTTPException(
            status_code=400,
            detail=f"Too many files. Maximum 10 allowed, received {len(files)}",
        )

    # ====================================================================
    # STEP 0: Check User Subscription and Limits
    # ====================================================================

    logger.info(f"🔍 Checking user subscription for user: {user_id}")

    # Check if user can perform comparison
    can_compare = await user_subscription_service.check_user_can_compare(
        user_id=user_id, files_count=len(files)
    )

    if not can_compare.get("allowed"):
        reason = can_compare.get("reason", "Comparison not allowed")
        logger.warning(f"❌ Comparison not allowed for user {user_id}: {reason}")
        raise HTTPException(status_code=403, detail=reason)

    # Get remaining comparisons and quotes per case
    remaining_comparisons = can_compare.get("remainingComparisons", 0)
    quotes_per_case = can_compare.get("quotesPerCase", 3)
    user_data = can_compare.get("user")

    logger.info(f"✅ User {user_id} can perform comparison")
    logger.info(f"   - Remaining comparisons: {remaining_comparisons}")
    logger.info(f"   - Quotes per case: {quotes_per_case}")
    logger.info(f"   - Files uploaded: {len(files)}")

    # Generate unique comparison ID for this batch
    comparison_id = _generate_comparison_id()
    logger.info(f"🆔 Generated comparison ID: {comparison_id}")

    # Generate unique job ID for progress tracking
    job_id = f"job_{uuid.uuid4().hex[:12]}"

    # Initialize progress tracking (start at 0%)
    progress_tracker.initialize_progress(
        job_id=job_id, total_files=len(files), comparison_id=comparison_id
    )
    logger.info(f"📊 Initialized progress tracking: {job_id}")

    # Create upload directory
    upload_dir = settings.UPLOAD_DIR
    os.makedirs(upload_dir, exist_ok=True)

    # Track processing
    saved_files = {}
    extracted_quotes = []
    extracted_models = []
    files_processed = []
    individual_pdf_ids = []  # NEW: Track MongoDB IDs of individual PDFs

    try:
        # ====================================================================
        # STEP 1: Process Each PDF in PARALLEL
        # ====================================================================

        async def process_single_file(file: UploadFile, file_index: int) -> None:
            """Save, extract, parse, and persist a single PDF."""
            # Validate
            if not file.filename.lower().endswith(".pdf"):
                logger.warning(f"⚠️  Skipping non-PDF file: {file.filename}")
                return

            logger.info(f"\n{'='*70}")
            logger.info(f"📄 Processing: {file.filename}")
            logger.info(f"{'='*70}")

            # Progress: 10-30% - File saving
            base_progress = 10 + (file_index * 20 / len(files))
            progress_tracker.update_progress(
                job_id=job_id,
                step_name="Processing PDFs",
                percentage=base_progress,
                details=f"Saving file {file_index + 1}/{len(files)}: {file.filename}",
                sub_step="Uploading and validating",
                current_file_index=file_index + 1,
                files_processed=file_index,
            )

            # Save
            file_info = await _save_uploaded_file(file, upload_dir)
            saved_files[file_info["file_name"]] = file_info

            # Progress: 30-50% - Text extraction
            extract_progress = 30 + (file_index * 20 / len(files))
            progress_tracker.update_progress(
                job_id=job_id,
                step_name="Extracting Text",
                percentage=extract_progress,
                details=f"Reading PDF {file_index + 1}/{len(files)}: {file.filename}",
                sub_step="Extracting text content from PDF",
                current_file_index=file_index + 1,
                files_processed=file_index,
            )

            # Extract text (run blocking IO in a worker thread)
            # Use the saved file path (file is already saved to disk)
            logger.info(f"📖 Extracting text from: {file.filename}")
            text_content = await asyncio.to_thread(
                pdf_extractor.extract_text_from_pdf,
                file_info["file_path"],
            )

            # Note: file_content is already read in _save_uploaded_file
            # No need to read again, it's in file_info["file_content"]

            # Progress: 50-70% - AI parsing
            parse_progress = 50 + (file_index * 20 / len(files))
            progress_tracker.update_progress(
                job_id=job_id,
                step_name="AI Analysis",
                percentage=parse_progress,
                details=f"Analyzing quote {file_index + 1}/{len(files)} with AI",
                sub_step=f"Parsing {file.filename}",
                current_file_index=file_index + 1,
                files_processed=file_index,
            )

            # Parse with AI
            logger.info(f"🤖 Parsing with AI: {file.filename}")
            extracted_data = await ai_parser.extract_structured_data_from_text(
                text_content, file_info["original_filename"]
            )

            # Collect: keep raw dict for response, model for ranker
            extracted_quotes.append(extracted_data)
            try:
                extracted_model = ExtractedQuoteData(**extracted_data)
                extracted_models.append(extracted_model)
            except Exception as model_err:
                logger.error(f"❌ Validation failed for {file.filename}: {model_err}")
                return

            files_processed.append(file_info["file_name"])

            # Progress: 70% - Saving to database
            save_progress = 70 + (file_index * 5 / len(files))
            progress_tracker.update_progress(
                job_id=job_id,
                step_name="Saving Data",
                percentage=save_progress,
                details=f"Storing quote {file_index + 1}/{len(files)} in database",
                sub_step="Persisting extracted data",
                current_file_index=file_index + 1,
                files_processed=file_index + 1,
            )

            # Save to MongoDB (for comparison processing)
            try:
                pdf_mongo_id = (
                    await enhanced_mongodb_service.save_individual_pdf_extraction(
                        comparison_id=comparison_id,
                        file_name=file_info["file_name"],
                        original_filename=file_info["original_filename"],
                        file_path=file_info["file_path"],
                        file_size=file_info["file_size"],
                        extracted_data=extracted_data,
                    )
                )
                individual_pdf_ids.append(pdf_mongo_id)
                logger.info(f"✅ Saved individual PDF to MongoDB: {pdf_mongo_id}")
            except Exception as mongo_error:
                logger.error(
                    f"⚠️  MongoDB save failed for {file.filename}: {mongo_error}"
                )

            # Save raw PDF as user document (separate from comparison)
            try:
                doc_id = await user_documents_service.save_document(
                    user_id=user_id,
                    original_filename=file_info["original_filename"],
                    pdf_binary=file_info.get("file_content"),
                    file_size=file_info["file_size"],
                )
                logger.info(f"✅ Saved user document: {file.filename} → {doc_id}")
            except Exception as doc_error:
                logger.error(
                    f"⚠️  User document save failed for {file.filename}: {doc_error}"
                )

            logger.info(f"✅ Successfully processed: {file.filename}")

        # Launch all tasks concurrently (with file index)
        tasks = [process_single_file(file, idx) for idx, file in enumerate(files)]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Check if we got any valid quotes
        if not extracted_models:
            raise HTTPException(
                status_code=400,
                detail="No valid insurance quotes could be extracted from the uploaded PDFs",
            )

        logger.info(f"\n✅ Successfully extracted {len(extracted_models)} quotes")

        # Update progress to 75% - preparing for AI ranking
        progress_tracker.update_progress(
            job_id=job_id,
            step_name="Preparing Analysis",
            percentage=75.0,
            details=f"Preparing {len(extracted_models)} quotes for comparison",
            sub_step="Organizing data for AI ranking",
            files_processed=len(extracted_models),
        )

        # ====================================================================
        # STEP 2: Generate Final Comparison
        # ====================================================================

        logger.info(f"\n{'='*70}")
        logger.info(f"🔀 Generating comparison for {len(extracted_models)} quotes")
        logger.info(f"{'='*70}")

        # Update progress to 80% - AI ranking in progress
        progress_tracker.update_progress(
            job_id=job_id,
            step_name="AI Ranking",
            percentage=80.0,
            details=f"AI is comparing and ranking {len(extracted_models)} quotes",
            sub_step="Analyzing coverage, pricing, and benefits",
            files_processed=len(extracted_models),
        )

        # Rank and compare using AI Ranker (pass the comparison_id)
        comparison_result = await quote_ranker.rank_and_compare_quotes(extracted_models, comparison_id=comparison_id)

        # Update progress to 90% - generating results
        progress_tracker.update_progress(
            job_id=job_id,
            step_name="Generating Results",
            percentage=90.0,
            details="Creating comprehensive comparison report",
            sub_step="Finalizing analysis and recommendations",
            files_processed=len(extracted_models),
        )

        # Build complete response
        response_data = {
            "comparison_id": comparison_id,
            "status": "completed",
            "total_quotes": len(extracted_models),
            # All sections from ranker
            **comparison_result,
            # Metadata
            "extracted_quotes": extracted_quotes,
            "files_processed": files_processed,
            "processing_timestamp": datetime.now().isoformat(),
            "processing_summary": {
                "total_files_uploaded": len(files),
                "successful_extractions": len(extracted_quotes),
                "failed_extractions": len(files) - len(extracted_quotes),
                "status": (
                    "All files processed successfully"
                    if len(extracted_quotes) == len(files)
                    else "Some files failed"
                ),
            },
        }

        # ====================================================================
        # STEP 3: Save Final Comparison Result to MongoDB
        # ====================================================================

        # Update progress to 95% - saving results
        progress_tracker.update_progress(
            job_id=job_id,
            step_name="Saving Results",
            percentage=95.0,
            details="Saving comparison results to database",
            sub_step="Persisting final comparison",
            files_processed=len(extracted_models),
        )

        try:
            # Check if MongoDB service is connected
            if enhanced_mongodb_service.comparisons_collection is None:
                logger.error("❌ MongoDB comparisons collection not initialized")
                raise HTTPException(
                    status_code=500, detail="Database connection not available"
                )

            # Get company_id for sharing with sub-accounts (optional - won't fail if None)
            # CRITICAL FIX: For main users without a company, use their own user_id as company_id
            # This allows sub-users (who have companyId pointing to parent's user_id) to find comparisons
            company_id = None
            try:
                company_id = await user_subscription_service.get_user_company_id(user_id)
                if company_id:
                    logger.info(f"🏢 User belongs to company: {company_id}")
                else:
                    # For main/parent users without a company, use their own user_id as company_id
                    # This enables sub-users to find their comparisons via companyId match
                    company_id = user_id
                    logger.info(f"🏢 Main user - using user_id as company_id for sub-user access: {company_id}")
            except Exception as company_error:
                # Don't fail if company_id lookup fails - use user_id as fallback
                logger.warning(f"⚠️  Could not get company_id, using user_id as fallback: {company_error}")
                company_id = user_id

            comparison_mongo_id = await enhanced_mongodb_service.save_comparison_result(
                comparison_id=comparison_id,
                comparison_data=response_data,
                pdf_count=len(individual_pdf_ids),
                user_id=user_id,
                company_id=company_id,  # Can be None - completely optional
            )
            logger.info(f"✅ Saved comparison result to MongoDB: {comparison_mongo_id}")
            logger.info(f"   Database: {enhanced_mongodb_service.database.name}")
            logger.info(f"   Collection: comparisons")
            logger.info(f"   Comparison ID: {comparison_id}")
            logger.info(f"   User ID: {user_id}")
            logger.info(f"   Company ID: {company_id or 'None (individual account)'}")
        except Exception as mongo_error:
            logger.error(f"❌ MongoDB save failed for comparison: {mongo_error}")
            import traceback

            logger.error(traceback.format_exc())
            # Don't fail the request, but log the error
            logger.warning("⚠️  Comparison completed but not saved to MongoDB")

        # ====================================================================
        # STEP 4: Record Comparison Usage
        # ====================================================================

        # Record comparison usage after successful comparison
        try:
            usage_recorded = await user_subscription_service.record_comparison(user_id)
            if usage_recorded:
                logger.info(f"✅ Recorded comparison usage for user {user_id}")
            else:
                logger.warning(
                    f"⚠️  Failed to record comparison usage for user {user_id}"
                )
        except Exception as usage_error:
            logger.error(f"❌ Error recording comparison usage: {usage_error}")
            # Don't fail the request if usage recording fails

        # ====================================================================
        # STEP 5: Return Response
        # ====================================================================

        logger.info(f"\n{'='*70}")
        logger.info(f"✅ COMPARISON COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"📊 Comparison ID: {comparison_id}")
        logger.info(f"👤 User ID: {user_id}")
        logger.info(f"📄 Files Processed: {len(extracted_quotes)}/{len(files)}")
        logger.info(
            f"💾 Saved to MongoDB: {len(individual_pdf_ids)} PDFs + 1 comparison"
        )

        # Get updated remaining comparisons
        updated_check = await user_subscription_service.check_user_can_compare(
            user_id=user_id, files_count=1  # Just to get updated count
        )
        updated_remaining = updated_check.get(
            "remainingComparisons", remaining_comparisons
        )

        logger.info(f"📊 Remaining comparisons: {updated_remaining}")
        logger.info(f"{'='*70}\n")

        # Mark progress as completed (100%)
        progress_tracker.mark_completed(
            job_id=job_id, comparison_id=comparison_id, result=response_data
        )

        # Add job_id and usage info to response
        response_data["job_id"] = job_id
        response_data["usage"] = {
            "remainingComparisons": updated_remaining,
            "quotesPerCase": quotes_per_case,
            "quotesUsed": len(files),
        }

        # Log activity for comparison creation
        try:
            if user_id:
                await activity_logs_service.create_activity_log(
                    user_id=user_id,
                    activity_type="comparison_created",
                    description=f"Created comparison with {len(files)} quotes",
                    metadata={
                        "comparison_id": comparison_id,
                        "num_quotes": len(files),
                        "providers": [q.get("insurer_name", "Unknown") for q in extracted_quotes]
                    }
                )
                logger.info(f"✅ Activity logged: comparison_created for user {user_id}")
        except Exception as log_err:
            logger.warning(f"⚠️  Failed to log activity: {log_err}")

        return JSONResponse(content=response_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        logger.exception(e)
        # Mark progress as error
        try:
            progress_tracker.mark_error(job_id, str(e))
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================================================
# COMPARISON RETRIEVAL ENDPOINT (For Frontend localStorage)
# ============================================================================


@router.get("/comparisons")
async def get_all_comparisons(
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Get all comparison data from database for frontend.
    Frontend should call this endpoint and store results in localStorage.
    
    Returns all comparisons accessible to the user:
    - Comparisons created by the user
    - Comparisons shared via company_id (if user belongs to a company)
    
    Args:
        user_id: User MongoDB ObjectId (from header or query)
    
    Returns:
        List of all comparison data (full comparison_data included)
    """
    try:
        # Get user ID from header or query parameter
        user_id = user_id or user_id_query

        if not user_id:
            raise HTTPException(
                status_code=400,
                detail="User ID is required. Please provide X-User-Id header or userId query parameter.",
            )

        logger.info(f"📊 Fetching all comparisons for user: {user_id}")

        # Get user's company_id (if they belong to a company) - optional, won't fail if None
        company_id = None
        try:
            company_id = await user_subscription_service.get_user_company_id(user_id)
            if company_id:
                logger.info(f"🏢 User belongs to company: {company_id} - will include shared comparisons")
        except Exception as company_error:
            # Don't fail if company_id lookup fails - it's optional
            logger.warning(f"⚠️  Could not get company_id (optional): {company_error}")
            company_id = None

        # Fetch all comparisons (user's own + company-shared)
        comparisons = await enhanced_mongodb_service.get_comparisons_by_user(
            user_id=user_id,
            company_id=company_id,
            limit=100,  # Get up to 100 comparisons
        )

        # Format response - return full comparison_data for localStorage
        formatted_comparisons = []
        for comp in comparisons:
            # Return the full comparison_data that frontend expects
            comparison_data = comp.get("comparison_data", {})
            
            # If comparison_data exists, use it; otherwise reconstruct from stored fields
            if comparison_data:
                formatted_comp = comparison_data.copy()
            else:
                # Fallback: reconstruct from stored fields
                formatted_comp = {
                    "comparison_id": comp.get("comparison_id"),
                    "status": comp.get("status", "completed"),
                    "total_quotes": comp.get("total_quotes", 0),
                    "summary": comp.get("summary", {}),
                    "key_differences": comp.get("key_differences", {}),
                    "side_by_side": comp.get("side_by_side", {}),
                    "data_table": comp.get("data_table", {}),
                    "analytics": comp.get("analytics", {}),
                    "provider_cards": comp.get("provider_cards", []),
                    "extracted_quotes": comp.get("extracted_quotes", []),
                    "charts": comp.get("charts", []),
                    "files_processed": comp.get("files_processed", []),
                    "processing_timestamp": comp.get("processing_timestamp"),
                }
            
            # Add metadata
            formatted_comp["_id"] = comp.get("id")
            formatted_comp["created_at"] = comp.get("created_at").isoformat() if comp.get("created_at") else None
            formatted_comp["user_id"] = comp.get("user_id")
            formatted_comp["company_id"] = comp.get("company_id")
            
            formatted_comparisons.append(formatted_comp)

        logger.info(f"✅ Found {len(formatted_comparisons)} comparisons for user {user_id}")

        return JSONResponse(
            content={
                "success": True,
                "count": len(formatted_comparisons),
                "comparisons": formatted_comparisons,
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error fetching comparisons: {e}")
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================================================
# USER DOCUMENTS ENDPOINTS (My Documents Section)
# ============================================================================


@router.get("/my-documents")
async def get_user_documents(
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Get all documents uploaded by a user.
    Shows all PDFs in "My Documents" section.

    Headers:
        X-User-Id: User MongoDB ObjectId (optional)

    Query Parameters:
        userId: Alternative way to pass user ID

    Returns:
        List of user documents (metadata only, no binary)
    """
    user_id = user_id or user_id_query

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="User ID is required. Please provide X-User-Id header or userId query parameter.",
        )

    try:
        documents = await user_documents_service.get_user_documents(user_id)
        return JSONResponse(content={"total": len(documents), "documents": documents})
    except Exception as e:
        logger.error(f"❌ Error getting user documents: {e}")
        import traceback

        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/my-documents/{document_id}/download")
async def download_user_document(
    document_id: str,
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Download a user document (raw PDF).

    Args:
        document_id: Document MongoDB ObjectId
        user_id: User MongoDB ObjectId (from header or query)

    Returns:
        Raw PDF file as download
    """
    user_id = user_id or user_id_query

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="User ID is required. Please provide X-User-Id header or userId query parameter.",
        )

    try:
        # Get document binary
        pdf_binary = await user_documents_service.get_document_binary(
            document_id, user_id
        )

        if not pdf_binary:
            raise HTTPException(
                status_code=404,
                detail=f"Document not found or access denied: {document_id}",
            )

        # Get document metadata for filename
        documents = await user_documents_service.get_user_documents(user_id)
        document = next((d for d in documents if d["id"] == document_id), None)

        if not document:
            raise HTTPException(
                status_code=404, detail=f"Document not found: {document_id}"
            )

        original_filename = document.get(
            "original_filename", f"document_{document_id}.pdf"
        )

        # Return PDF as download
        return Response(
            content=pdf_binary,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{original_filename}"',
                "Content-Length": str(len(pdf_binary)),
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error downloading document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# PDF REPORT GENERATION ENDPOINT
# ============================================================================

@router.get("/comparison/{comparison_id}/pdf")
async def download_comparison_pdfs(
    comparison_id: str,
    report_type: Optional[str] = Query("both", description="Type of report: 'strategic-memo', 'detailed-comparison', or 'both'"),
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Generate and download PDF reports for a comparison.
    
    By default, returns strategic memo. Use report_type query parameter to specify:
    - 'strategic-memo': 1-page strategic memo only
    - 'detailed-comparison': Detailed comparison report only
    - 'both': Returns JSON with download URLs for both (frontend handles downloads)

    Args:
        comparison_id: Comparison ID from compare-quotes response
        report_type: Type of report to download (default: 'both')
        user_id: User MongoDB ObjectId (optional, for validation)

    Returns:
        PDF file(s) or JSON with download URLs
    """
    try:
        logger.info(f"📄 Generating PDF for comparison: {comparison_id}, type: {report_type}")

        # Fetch comparison data
        comparison_data = await enhanced_mongodb_service.get_comparison_result(
            comparison_id
        )

        if not comparison_data:
            raise HTTPException(
                status_code=404, detail=f"Comparison not found: {comparison_id}"
            )

        # Extract comparison data
        comp_data = comparison_data.get("comparison_data", {})

        if not comp_data:
            comp_data = {
                "comparison_id": comparison_id,
                "total_quotes": comparison_data.get("total_pdfs", 0),
                "summary": comparison_data.get("summary", {}),
                "key_differences": comparison_data.get("key_differences", {}),
                "side_by_side": comparison_data.get("side_by_side", {}),
                "data_table": comparison_data.get("data_table", {}),
                "analytics": comparison_data.get("analytics", {}),
                "provider_cards": comparison_data.get("provider_cards", []),
            }

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # If both reports requested, generate both PDFs and return JSON with base64 data
        if report_type == "both":
            # Generate both PDFs
            strategic_memo_buffer = pdf_generator_service.generate_strategic_memo_pdf(
                comparison_data=comp_data, comparison_id=comparison_id
            )
            detailed_comparison_buffer = pdf_generator_service.generate_detailed_comparison_pdf(
                comparison_data=comp_data, comparison_id=comparison_id
            )
            
            strategic_memo_bytes = strategic_memo_buffer.getvalue()
            detailed_comparison_bytes = detailed_comparison_buffer.getvalue()
            
            strategic_filename = f"Strategic_Memo_{comparison_id}_{timestamp}.pdf"
            detailed_filename = f"HAKEM_AI_Detailed_Technical_Comparison_{comparison_id}_{timestamp}.pdf"
            
            # Convert to base64 for JSON response
            import base64
            strategic_b64 = base64.b64encode(strategic_memo_bytes).decode('utf-8')
            detailed_b64 = base64.b64encode(detailed_comparison_bytes).decode('utf-8')
            
            logger.info(f"✅ Both PDFs generated: {strategic_filename} ({len(strategic_memo_bytes)} bytes), {detailed_filename} ({len(detailed_comparison_bytes)} bytes)")
            
            # Return JSON with both PDFs
            return JSONResponse(content={
                "success": True,
                "comparison_id": comparison_id,
                "reports": {
                    "strategic_memo": {
                        "filename": strategic_filename,
                        "data": strategic_b64,
                        "size": len(strategic_memo_bytes)
                    },
                    "detailed_comparison": {
                        "filename": detailed_filename,
                        "data": detailed_b64,
                        "size": len(detailed_comparison_bytes)
                    }
                },
                "message": "Both reports generated successfully"
            })

        # Generate the requested PDF
        if report_type == "strategic-memo":
            pdf_buffer = pdf_generator_service.generate_strategic_memo_pdf(
                comparison_data=comp_data, comparison_id=comparison_id
            )
            filename = f"Strategic_Memo_{comparison_id}_{timestamp}.pdf"
        elif report_type == "detailed-comparison":
            pdf_buffer = pdf_generator_service.generate_detailed_comparison_pdf(
                comparison_data=comp_data, comparison_id=comparison_id
            )
            filename = f"HAKEM_AI_Detailed_Technical_Comparison_{comparison_id}_{timestamp}.pdf"
        else:
            # Default: strategic memo
            pdf_buffer = pdf_generator_service.generate_strategic_memo_pdf(
                comparison_data=comp_data, comparison_id=comparison_id
            )
            filename = f"Strategic_Memo_{comparison_id}_{timestamp}.pdf"

        # Get PDF bytes
        pdf_bytes = pdf_buffer.getvalue()

        logger.info(f"✅ PDF generated: {filename} ({len(pdf_bytes)} bytes)")

        # Return PDF with proper headers for download
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(pdf_bytes)),
                "Content-Type": "application/pdf",
                "Access-Control-Expose-Headers": "Content-Disposition, Content-Length",
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error generating PDF: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")


@router.get("/comparison/{comparison_id}/pdf/strategic-memo")
async def download_strategic_memo_pdf(
    comparison_id: str,
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Generate and download 1-page Strategic Memo PDF report.
    
    This is a high-level executive brief optimized for decision makers.

    Args:
        comparison_id: Comparison ID from compare-quotes response
        user_id: User MongoDB ObjectId (optional, for validation)

    Returns:
        PDF file as download (Strategic Memo)
    """
    try:
        logger.info(f"📄 Generating Strategic Memo PDF for comparison: {comparison_id}")

        # Fetch comparison data
        comparison_data = await enhanced_mongodb_service.get_comparison_result(
            comparison_id
        )

        if not comparison_data:
            raise HTTPException(
                status_code=404, detail=f"Comparison not found: {comparison_id}"
            )

        # Extract comparison data
        comp_data = comparison_data.get("comparison_data", {})

        if not comp_data:
            comp_data = {
                "comparison_id": comparison_id,
                "total_quotes": comparison_data.get("total_pdfs", 0),
                "summary": comparison_data.get("summary", {}),
                "key_differences": comparison_data.get("key_differences", {}),
                "side_by_side": comparison_data.get("side_by_side", {}),
                "data_table": comparison_data.get("data_table", {}),
                "analytics": comparison_data.get("analytics", {}),
                "provider_cards": comparison_data.get("provider_cards", []),
            }

        # Generate Strategic Memo PDF
        pdf_buffer = pdf_generator_service.generate_strategic_memo_pdf(
            comparison_data=comp_data, comparison_id=comparison_id
        )

        # Get PDF bytes
        pdf_bytes = pdf_buffer.getvalue()

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Strategic_Memo_{comparison_id}_{timestamp}.pdf"

        logger.info(
            f"✅ Strategic Memo PDF generated: {filename} ({len(pdf_bytes)} bytes)"
        )

        # Return PDF as download with proper headers
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Content-Length": str(len(pdf_bytes)),
                "Content-Type": "application/pdf",
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error generating Strategic Memo PDF: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to generate Strategic Memo PDF: {str(e)}")


@router.get("/comparison/{comparison_id}/pdf/detailed-comparison")
async def download_detailed_comparison_pdf(
    comparison_id: str,
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Generate and download Detailed Comparison PDF report.
    
    This is a comprehensive technical report with all sections:
    - Detailed Technical Analysis
    - Key Differences
    - Data Table
    - Side-by-Side Comparison
    - Analytics & Statistics

    Args:
        comparison_id: Comparison ID from compare-quotes response
        user_id: User MongoDB ObjectId (optional, for validation)

    Returns:
        PDF file as download (Detailed Comparison)
    """
    try:
        logger.info(f"📄 Generating Detailed Comparison PDF for comparison: {comparison_id}")

        # Fetch comparison data
        comparison_data = await enhanced_mongodb_service.get_comparison_result(
            comparison_id
        )

        if not comparison_data:
            raise HTTPException(
                status_code=404, detail=f"Comparison not found: {comparison_id}"
            )

        # Extract comparison data
        comp_data = comparison_data.get("comparison_data", {})

        if not comp_data:
            comp_data = {
                "comparison_id": comparison_id,
                "total_quotes": comparison_data.get("total_pdfs", 0),
                "summary": comparison_data.get("summary", {}),
                "key_differences": comparison_data.get("key_differences", {}),
                "side_by_side": comparison_data.get("side_by_side", {}),
                "data_table": comparison_data.get("data_table", {}),
                "analytics": comparison_data.get("analytics", {}),
                "provider_cards": comparison_data.get("provider_cards", []),
            }

        # Generate Detailed Comparison PDF
        pdf_buffer = pdf_generator_service.generate_detailed_comparison_pdf(
            comparison_data=comp_data, comparison_id=comparison_id
        )

        # Get PDF bytes
        pdf_bytes = pdf_buffer.getvalue()

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"HAKEM_AI_Detailed_Technical_Comparison_{comparison_id}_{timestamp}.pdf"

        logger.info(
            f"✅ Detailed Comparison PDF generated: {filename} ({len(pdf_bytes)} bytes)"
        )

        # Return PDF as download with proper headers
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Content-Length": str(len(pdf_bytes)),
                "Content-Type": "application/pdf",
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error generating Detailed Comparison PDF: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to generate Detailed Comparison PDF: {str(e)}")


@router.delete("/my-documents/{document_id}")
async def delete_user_document(
    document_id: str,
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Delete a user document.

    Args:
        document_id: Document MongoDB ObjectId
        user_id: User MongoDB ObjectId (from header or query)

    Returns:
        Success status
    """
    user_id = user_id or user_id_query

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="User ID is required. Please provide X-User-Id header or userId query parameter.",
        )

    try:
        deleted = await user_documents_service.delete_document(document_id, user_id)

        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"Document not found or access denied: {document_id}",
            )

        return JSONResponse(
            content={
                "success": True,
                "message": f"Document {document_id} deleted successfully",
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error deleting document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# HEALTH CHECK
# ============================================================================


@router.get("/health")
async def health_check():
    """
    Health check endpoint.
    """
    return JSONResponse(
        content={
            "status": "healthy",
            "version": "3.2.0",
            "features": [
                "✅ Individual PDF storage in MongoDB",
                "✅ Linked PDFs via comparison_id",
                "✅ Separate comparison result storage",
                "✅ No data mixing between PDFs",
                "✅ Complete retrieval methods",
                "✅ Enhanced MongoDB Service v2.0",
                "✅ AI Parser v6.0",
                "✅ AI Ranker v3.0 (Full Lists Support)",
                "✅ User Subscription Validation",
            ],
            "timestamp": datetime.now().isoformat(),
        }
    )


@router.get("/test-user/{user_id}")
async def test_user_lookup(user_id: str):
    """
    Test endpoint to verify user lookup and subscription status.
    Useful for debugging user subscription service.
    """
    try:
        # Check user subscription
        can_compare = await user_subscription_service.check_user_can_compare(
            user_id=user_id, files_count=1
        )

        return JSONResponse(
            content={
                "user_id": user_id,
                "can_compare": can_compare,
                "timestamp": datetime.now().isoformat(),
            }
        )
    except Exception as e:
        logger.error(f"❌ Error testing user lookup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debug/databases")
async def debug_databases():
    """
    Debug endpoint to list all databases and find where users are stored.
    """
    try:
        from bson import ObjectId

        client = user_subscription_service.client
        if not client:
            return JSONResponse(
                content={
                    "error": "MongoDB client not connected",
                    "timestamp": datetime.now().isoformat(),
                }
            )

        # List all databases
        db_list = await client.list_database_names()

        result = {
            "databases": [],
            "current_database": (
                user_subscription_service.database.name
                if user_subscription_service.database
                else None
            ),
            "timestamp": datetime.now().isoformat(),
        }

        # Check each database for users collection
        for db_name in db_list:
            if db_name in ["admin", "config", "local"]:
                continue  # Skip system databases

            db = client[db_name]
            collections = await db.list_collection_names()

            db_info = {
                "name": db_name,
                "collections": collections,
                "has_users": "users" in collections,
            }

            if "users" in collections:
                users_col = db.users
                user_count = await users_col.count_documents({})
                db_info["user_count"] = user_count

                # Try to find the specific user
                try:
                    user = await users_col.find_one(
                        {"_id": ObjectId("690b5e52447df54e13f39f1e")}
                    )
                    if user:
                        db_info["has_target_user"] = True
                        db_info["user_email"] = user.get("email", "N/A")
                        db_info["user_username"] = user.get("username", "N/A")
                        db_info["account_status"] = user.get("accountStatus", "N/A")
                        db_info["subscription_plan"] = user.get(
                            "subscriptionPlan", "N/A"
                        )
                    else:
                        db_info["has_target_user"] = False
                except Exception as e:
                    db_info["user_lookup_error"] = str(e)

            result["databases"].append(db_info)

        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"❌ Error debugging databases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# HAKIM SCORE MANAGEMENT ENDPOINTS (Admin)
# ============================================================================

from app.services.hakim_score_service import hakim_score_service
from pydantic import BaseModel, Field, validator
from typing import Optional, List


class HakimScoreRequest(BaseModel):
    """Request model for creating/updating Hakim scores."""
    company_name: str = Field(..., min_length=1, description="Company name")
    score: float = Field(..., ge=0.0, le=1.0, description="Score value (0.0 to 1.0, 0.0 allowed to disable)")
    tier: str = Field(..., description="Tier name (Premium, Strong, Solid, Baseline, Challenged, Standard, Disabled)")
    rank: int = Field(..., ge=1, description="Rank number (1 = highest)")
    aliases: Optional[List[str]] = Field(default_factory=list, description="Company name aliases/variations")


class HakimScoreUpdateRequest(BaseModel):
    """Request model for updating Hakim scores (partial update)."""
    score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Score value (0.0 to 1.0, 0.0 allowed to disable)")
    tier: Optional[str] = Field(None, description="Tier name")
    rank: Optional[int] = Field(None, ge=1, description="Rank number")
    aliases: Optional[List[str]] = Field(None, description="Company name aliases")


class ScoreUpdateRequest(BaseModel):
    """Simple request model for updating just the score."""
    score: float = Field(..., ge=0.0, le=1.0, description="Hakim score value (0.0 to 1.0, 0.0 allowed to disable)")


class BulkScoreUpdateItem(BaseModel):
    """Single item in bulk score update."""
    company_name: str = Field(..., min_length=1, description="Company name")
    score: float = Field(..., ge=0.0, le=1.0, description="Hakim score value (0.0 to 1.0)")


class HakimScoreBulkUpdateRequest(BaseModel):
    """Request model for bulk updating scores (optimized for scoring page)."""
    updates: List[BulkScoreUpdateItem] = Field(..., description="List of score updates")


@router.post("/admin/hakim-scores/initialize")
async def initialize_hakim_scores():
    """
    Initialize Hakim scores from HAKIM_SCORE dictionary in ai_ranker.py.
    This endpoint allows admins to populate the database with default scores.
    Only needs to be run once when setting up a new database.
    
    Returns:
        Summary of initialization (created, updated, failed counts)
    """
    try:
        logger.info("🔄 Initializing Hakim scores from HAKIM_SCORE dictionary...")
        
        # Import HAKIM_SCORE from ai_ranker
        from app.services.ai_ranker import HAKIM_SCORE
        
        # Extract unique companies
        companies_by_rank = {}
        for company_name, data in HAKIM_SCORE.items():
            rank = data.get('rank', 999)
            score = data.get('score', 0.75)
            tier = data.get('tier', 'Standard')
            
            if rank not in companies_by_rank:
                companies_by_rank[rank] = {
                    'names': [],
                    'score': score,
                    'tier': tier,
                    'rank': rank
                }
            companies_by_rank[rank]['names'].append(company_name)
        
        # Create unique companies list
        scores_data = []
        for rank in sorted(companies_by_rank.keys()):
            company_data = companies_by_rank[rank]
            names = company_data['names']
            
            # Prefer English names over Arabic
            english_names = [n for n in names if not any('\u0600' <= c <= '\u06FF' for c in n)]
            if english_names:
                candidates = [n for n in english_names if len(n) > 3]
                main_name = max(candidates, key=len) if candidates else max(english_names, key=len)
            else:
                main_name = max(names, key=len)
            
            aliases = [name for name in names if name != main_name]
            
            scores_data.append({
                'company_name': main_name,
                'score': company_data['score'],
                'tier': company_data['tier'],
                'rank': company_data['rank'],
                'aliases': aliases
            })
        
        # Bulk create/update
        logger.info(f"💾 Saving {len(scores_data)} companies to database...")
        result = await hakim_score_service.bulk_create_or_update(scores_data)
        
        logger.info(f"✅ Initialization complete: Created={result['created']}, Updated={result['updated']}")
        
        return JSONResponse(content={
            "success": True,
            "message": f"Initialized {result['created'] + result['updated']} companies",
            "created": result['created'],
            "updated": result['updated'],
            "failed": result['failed'],
            "total": result['total']
        })
        
    except Exception as e:
        logger.error(f"❌ Error initializing Hakim scores: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to initialize Hakim scores: {str(e)}")


@router.get("/admin/hakim-scores")
async def get_all_hakim_scores(
    sort_by: Optional[str] = Query("company_name", description="Sort field: rank, company_name, score, tier"),
    sort_order: Optional[str] = Query("asc", description="Sort order: asc or desc"),
    include_zero: Optional[bool] = Query(True, description="Include companies with score = 0")
):
    """
    Get all Hakim scores for admin scoring page.
    Returns all companies in simple format: company_name and score.
    Optimized for frontend table where admin just enters scores.
    
    Query Parameters:
        sort_by: Field to sort by (rank, company_name, score, tier) - Default: company_name
        sort_order: Sort order (asc or desc) - Default: asc
        include_zero: Whether to include companies with score = 0.0 - Default: true
    
    Returns:
        List of all companies with their current scores, ready for table display
        Format: [{"company_name": "...", "score": 0.88, "score_display": 88.0, "tier": "Strong", ...}, ...]
    """
    try:
        # Validate and convert sort parameters
        valid_sort_fields = ["rank", "company_name", "score", "tier", "updated_at"]
        if sort_by not in valid_sort_fields:
            sort_by = "company_name"  # Default to alphabetical
        
        sort_direction = 1 if sort_order.lower() == "asc" else -1
        
        scores = await hakim_score_service.get_all_hakim_scores(
            sort_by=sort_by,
            sort_order=sort_direction,
            include_zero_scores=include_zero
        )
        
        return JSONResponse(
            content={
                "success": True,
                "count": len(scores),
                "total_companies": len(scores),
                "companies": scores,  # Changed from "scores" to "companies" for clarity
                "sort_by": sort_by,
                "sort_order": sort_order
            }
        )
    except Exception as e:
        logger.error(f"❌ Error getting Hakim scores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# IMPORTANT: Specific routes must come BEFORE parameterized routes
# to avoid route conflicts (e.g., "bulk-update" matching {company_name})

@router.put("/admin/hakim-scores/bulk-update")
async def bulk_update_scores_only(updates: HakimScoreBulkUpdateRequest):
    """
    Bulk update only scores (optimized for scoring page).
    Tier is automatically determined from score for each company.
    More efficient when only scores need to be updated.
    Supports setting scores to 0.0 to disable companies.
    
    Args:
        updates: List of updates with company_name and score
                Format: {"updates": [{"company_name": "GIG", "score": 0.88}, ...]}
        
    Returns:
        Bulk update results with auto-determined tiers
    """
    try:
        # Convert Pydantic models to dicts
        updates_list = [
            {
                "company_name": item.company_name,
                "score": item.score
            }
            for item in updates.updates
        ]
        
        result = await hakim_score_service.bulk_update_scores(updates_list)
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Bulk update completed: {result['updated']} updated, {result['failed']} failed",
                "summary": {
                    "updated": result['updated'],
                    "failed": result['failed'],
                    "total": result['total']
                },
                "results": result.get('results', [])
            }
        )
    except Exception as e:
        logger.error(f"❌ Error in bulk update: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/hakim-scores/bulk")
async def bulk_create_or_update_hakim_scores(scores: List[HakimScoreRequest]):
    """
    Bulk create or update multiple Hakim scores (Admin endpoint).
    Supports setting scores to 0.0 to disable companies.
    
    Args:
        scores: List of Hakim score data
        
    Returns:
        Bulk operation results with detailed status for each company
    """
    try:
        scores_data = [
            {
                "company_name": s.company_name,
                "score": s.score,
                "tier": s.tier,
                "rank": s.rank,
                "aliases": s.aliases
            }
            for s in scores
        ]
        
        result = await hakim_score_service.bulk_create_or_update(scores_data)
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Bulk operation completed: {result['created']} created, {result['updated']} updated, {result['failed']} failed",
                "summary": {
                    "created": result['created'],
                    "updated": result['updated'],
                    "failed": result['failed'],
                    "total": result['total']
                },
                "results": result.get('results', [])
            }
        )
    except Exception as e:
        logger.error(f"❌ Error in bulk operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/hakim-scores/search/{query}")
async def search_hakim_scores(query: str):
    """
    Search companies by name or alias (Admin endpoint).
    
    Args:
        query: Search query
        
    Returns:
        List of matching companies
    """
    try:
        results = await hakim_score_service.search_companies(query)
        
        return JSONResponse(
            content={
                "success": True,
                "count": len(results),
                "results": results
            }
        )
    except Exception as e:
        logger.error(f"❌ Error searching Hakim scores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/hakim-scores/{company_name}")
async def get_hakim_score(company_name: str):
    """
    Get Hakim score for a specific company (Admin endpoint).
    
    Args:
        company_name: Company name
        
    Returns:
        Hakim score data for the company
    """
    try:
        score = await hakim_score_service.get_hakim_score(company_name)
        
        if not score:
            raise HTTPException(
                status_code=404,
                detail=f"Hakim score not found for company: {company_name}"
            )
        
        return JSONResponse(content={"success": True, "score": score})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting Hakim score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/hakim-scores")
async def create_or_update_hakim_score(score_data: HakimScoreRequest):
    """
    Create or update Hakim score for a company (Admin endpoint).
    
    Args:
        score_data: Hakim score data
        
    Returns:
        Created/updated Hakim score
    """
    try:
        result = await hakim_score_service.create_or_update_hakim_score(
            company_name=score_data.company_name,
            score=score_data.score,
            tier=score_data.tier,
            rank=score_data.rank,
            aliases=score_data.aliases
        )
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Hakim score {'created' if 'created_at' in result and result.get('created_at') == result.get('updated_at') else 'updated'} for {score_data.company_name}",
                "score": result
            }
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Error creating/updating Hakim score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/admin/hakim-scores/{company_name}/score")
async def update_company_score(
    company_name: str,
    score_data: ScoreUpdateRequest
):
    """
    Update ONLY the Hakim score for a company (Simplified for scoring page).
    Tier is automatically determined from score.
    Existing rank and aliases are preserved.
    
    Args:
        company_name: Company name
        score_data: Score value (0.0 to 1.0, 0.0 allowed to disable)
        
    Returns:
        Updated Hakim score with auto-determined tier
    """
    try:
        result = await hakim_score_service.update_score_only(
            company_name=company_name,
            score=score_data.score
        )
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Score updated for {company_name}",
                "score": result
            }
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Error updating score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/admin/hakim-scores/{company_name}")
async def update_hakim_score(
    company_name: str,
    update_data: HakimScoreUpdateRequest
):
    """
    Update specific fields of a Hakim score (Full update - Admin endpoint).
    Supports setting score to 0.0 to disable/zero out a company.
    Tier is auto-determined from score if not provided.
    
    Args:
        company_name: Company name
        update_data: Fields to update (score can be 0.0)
        
    Returns:
        Updated Hakim score
    """
    try:
        result = await hakim_score_service.update_hakim_score(
            company_name=company_name,
            score=update_data.score,
            tier=update_data.tier,
            rank=update_data.rank,
            aliases=update_data.aliases
        )
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Hakim score updated for {company_name}",
                "score": result
            }
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Error updating Hakim score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/admin/hakim-scores/{company_name}")
async def delete_hakim_score(company_name: str):
    """
    Delete Hakim score for a company (Admin endpoint).
    After deletion, system will fallback to hardcoded values if available.
    
    Args:
        company_name: Company name (case-insensitive)
        
    Returns:
        Success status
    """
    try:
        deleted = await hakim_score_service.delete_hakim_score(company_name)
        
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"Hakim score not found for company: {company_name}"
            )
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Hakim score deleted for {company_name}. System will use hardcoded fallback if available.",
                "company_name": company_name
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error deleting Hakim score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/hakim-scores/{company_name}/set-zero")
async def set_score_to_zero(company_name: str):
    """
    Set a company's score to 0.0 (disable/zero out).
    Convenience endpoint for scoring page.
    
    Args:
        company_name: Company name
        
    Returns:
        Updated score with zero value
    """
    try:
        result = await hakim_score_service.update_hakim_score(
            company_name=company_name,
            score=0.0,
            tier="Disabled"
        )
        
        formatted_result = {
            "id": result.get("id"),
            "company_name": result.get("company_name"),
            "score": 0.0,
            "score_display": 0.0,
            "tier": "Disabled",
            "rank": result.get("rank", 999),
            "is_zero": True
        }
        
        return JSONResponse(
            content={
                "success": True,
                "message": f"Score set to 0.0 for {company_name}",
                "score": formatted_result
            }
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Error setting score to zero: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ACTIVITY LOGS ENDPOINTS (Public - for user actions)
# ============================================================================

from pydantic import BaseModel

class ActivityLogCreate(BaseModel):
    """Request model for creating activity logs"""
    userId: str
    activityType: str
    description: str
    userEmail: Optional[str] = None
    username: Optional[str] = None
    ipAddress: Optional[str] = None
    userAgent: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@router.post("/activity-logs/create")
async def create_activity_log(log_data: ActivityLogCreate):
    """
    Create a new activity log entry (Public endpoint for user actions).
    
    Request Body:
        userId: User ID (required)
        activityType: Type of activity (e.g., login, logout, comparison_created)
        description: Description of the activity
        userEmail: User email (optional)
        username: Username (optional)
        ipAddress: IP address (optional)
        userAgent: User agent (optional)
        metadata: Additional metadata (optional)
    
    Returns:
        Success message with log ID
    """
    try:
        logger.info(f"📝 Creating activity log: {log_data.activityType} for user {log_data.userId}")
        
        log_id = await activity_logs_service.create_activity_log(
            user_id=log_data.userId,
            activity_type=log_data.activityType,
            description=log_data.description,
            user_email=log_data.userEmail,
            username=log_data.username,
            ip_address=log_data.ipAddress,
            user_agent=log_data.userAgent,
            metadata=log_data.metadata
        )
        
        logger.info(f"✅ Activity log created: {log_id}")
        
        return JSONResponse(content={
            "success": True,
            "message": "Activity logged successfully",
            "logId": log_id
        })
        
    except Exception as e:
        logger.error(f"❌ Error creating activity log: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Don't fail the request if logging fails
        return JSONResponse(content={
            "success": False,
            "message": "Failed to log activity",
            "error": str(e)
        }, status_code=200)  # Return 200 so it doesn't break user flow


# ============================================================================
# ADMIN ACTIVITY LOGS ENDPOINTS
# ============================================================================

@router.get("/admin/activity-logs")
async def get_activity_logs(
    page: Optional[int] = Query(1, ge=1, description="Page number"),
    limit: Optional[int] = Query(50, ge=1, le=100, description="Number of logs per page"),
    userId: Optional[str] = Query(None, alias="userId", description="Filter by user ID"),
    activityType: Optional[str] = Query(None, alias="activityType", description="Filter by activity type"),
    startDate: Optional[str] = Query(None, alias="startDate", description="Start date filter (ISO format)"),
    endDate: Optional[str] = Query(None, alias="endDate", description="End date filter (ISO format)"),
    search: Optional[str] = Query(None, description="Search in username, email, description"),
):
    """
    Get activity logs with filtering and pagination (Admin endpoint).
    
    Query Parameters:
        page: Page number (default: 1)
        limit: Number of logs per page (default: 50, max: 100)
        userId: Filter by user ID (optional)
        activityType: Filter by activity type (optional, use "All Types" for no filter)
        startDate: Start date filter in ISO format (optional)
        endDate: End date filter in ISO format (optional)
        search: Search query for username, email, or description (optional)
    
    Returns:
        Activity logs with pagination metadata
    """
    try:
        # Ensure defaults are set
        page_num = page if page is not None else 1
        limit_num = limit if limit is not None else 50
        
        logger.info(f"📊 Fetching activity logs: page={page_num}, limit={limit_num}, userId={userId}, activityType={activityType}")
        
        # Ensure activity logs service is connected
        if activity_logs_service.activity_logs_collection is None:
            logger.warning("⚠️  Activity logs collection not initialized, attempting to connect...")
            try:
                await activity_logs_service.connect()
                logger.info("✅ Activity logs service connected successfully")
            except Exception as connect_error:
                logger.error(f"❌ Failed to connect activity logs service: {connect_error}")
                # Return empty result instead of failing
                return JSONResponse(content={
                    "logs": [],
                    "total": 0,
                    "page": page_num,
                    "totalPages": 1
                })
        else:
            logger.info("✅ Activity logs service already connected")
        
        # Parse dates if provided
        start_date = None
        end_date = None
        if startDate:
            try:
                start_date = datetime.fromisoformat(startDate.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid startDate format. Use ISO format.")
        if endDate:
            try:
                end_date = datetime.fromisoformat(endDate.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid endDate format. Use ISO format.")
        
        logger.info(f"🔍 Calling activity_logs_service.get_activity_logs...")
        result = await activity_logs_service.get_activity_logs(
            page=page_num,
            limit=limit_num,
            user_id=userId,
            activity_type=activityType,
            start_date=start_date,
            end_date=end_date,
            search=search
        )
        
        logger.info(f"✅ Retrieved {len(result.get('logs', []))} logs (total: {result.get('total', 0)})")
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting activity logs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Return empty result instead of failing completely to prevent frontend errors
        return JSONResponse(content={
            "logs": [],
            "total": 0,
            "page": page_num if 'page_num' in locals() else 1,
            "totalPages": 1,
            "error": f"Failed to retrieve activity logs: {str(e)}"
        }, status_code=200)  # Return 200 with empty data instead of 500


@router.get("/admin/users/{userId}/activity-logs")
async def get_user_activity_logs(
    userId: str,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Number of logs per page"),
):
    """
    Get activity logs for a specific user (Admin endpoint).
    
    Args:
        userId: User ID
        page: Page number (default: 1)
        limit: Number of logs per page (default: 50, max: 100)
    
    Returns:
        Activity logs for the user with pagination metadata
    """
    try:
        result = await activity_logs_service.get_user_activity_logs(
            user_id=userId,
            page=page,
            limit=limit
        )
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting user activity logs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to get user activity logs: {str(e)}")


@router.get("/admin/activity-logs/statistics")
async def get_activity_statistics(
    startDate: Optional[str] = Query(None, alias="startDate", description="Start date filter (ISO format)"),
    endDate: Optional[str] = Query(None, alias="endDate", description="End date filter (ISO format)"),
):
    """
    Get activity statistics (Admin endpoint).
    
    Query Parameters:
        startDate: Start date filter in ISO format (optional)
        endDate: End date filter in ISO format (optional)
    
    Returns:
        Activity statistics including:
        - totalActivities
        - activitiesByType
        - topUsers
        - activitiesByDay
    """
    try:
        # Ensure activity logs service is connected
        if activity_logs_service.activity_logs_collection is None:
            logger.warning("⚠️  Activity logs collection not initialized, attempting to connect...")
            try:
                await activity_logs_service.connect()
                logger.info("✅ Activity logs service connected successfully")
            except Exception as connect_error:
                logger.error(f"❌ Failed to connect activity logs service: {connect_error}")
                # Return empty statistics instead of failing
                return JSONResponse(content={
                    "totalActivities": 0,
                    "activitiesByType": {},
                    "topUsers": [],
                    "activitiesByDay": []
                })
        else:
            logger.info("✅ Activity logs service already connected")
        
        # Parse dates if provided
        start_date = None
        end_date = None
        if startDate:
            try:
                start_date = datetime.fromisoformat(startDate.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid startDate format. Use ISO format.")
        if endDate:
            try:
                end_date = datetime.fromisoformat(endDate.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid endDate format. Use ISO format.")
        
        result = await activity_logs_service.get_activity_statistics(
            start_date=start_date,
            end_date=end_date
        )
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting activity statistics: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to get activity statistics: {str(e)}")


@router.get("/admin/activity-logs/recent")
async def get_recent_activity_logs(
    limit: int = Query(10, ge=1, le=100, description="Number of recent logs to return"),
):
    """
    Get recent activity logs (Admin endpoint).
    
    Query Parameters:
        limit: Number of recent logs to return (default: 10, max: 100)
    
    Returns:
        List of recent activity logs
    """
    try:
        result = await activity_logs_service.get_recent_activity_logs(limit=limit)
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting recent activity logs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to get recent activity logs: {str(e)}")


@router.get("/")
async def root():
    """API root endpoint with documentation."""
    return JSONResponse(
        content={
            "message": "Insurance Quote Comparison API v3.2 - Enhanced with Individual PDF Storage",
            "documentation": "/docs",
            "health": "/api/health",
            "main_endpoint": "/api/compare-quotes",
            "new_features": [
                "Individual PDF storage",
                "Enhanced MongoDB integration",
                "Complete retrieval methods",
                "No data mixing",
                "Full lists in side-by-side comparison",
                "Admin Hakim Score Management",
                "Admin Activity Logs Management",
            ],
        }
    )
