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
from datetime import datetime
from pathlib import Path
import asyncio

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
            company_id = None
            try:
                company_id = await user_subscription_service.get_user_company_id(user_id)
                if company_id:
                    logger.info(f"🏢 User belongs to company: {company_id}")
            except Exception as company_error:
                # Don't fail if company_id lookup fails - it's optional
                logger.warning(f"⚠️  Could not get company_id (optional): {company_error}")
                company_id = None

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
async def download_comparison_pdf(
    comparison_id: str,
    user_id: Optional[str] = Header(None, alias="X-User-Id"),
    user_id_query: Optional[str] = Query(None, alias="userId"),
):
    """
    Generate and download comprehensive PDF report for a comparison.

    Includes all sections:
    - Executive Summary
    - Key Differences
    - Data Table
    - Side-by-Side Comparison
    - Analytics & Statistics

    Args:
        comparison_id: Comparison ID from compare-quotes response
        user_id: User MongoDB ObjectId (optional, for validation)

    Returns:
        PDF file as download
    """
    try:
        logger.info(f"📄 Generating PDF for comparison: {comparison_id}")

        # Fetch comparison data from MongoDB
        comparison_doc = await enhanced_mongodb_service.get_comparison_result(
            comparison_id
        )

        if not comparison_doc:
            raise HTTPException(
                status_code=404, detail=f"Comparison not found: {comparison_id}"
            )

        # Extract comparison data
        comparison_data = comparison_doc.get("comparison_data", {})

        # If comparison_data is empty, try to reconstruct from document fields
        if not comparison_data:
            comparison_data = {
                "comparison_id": comparison_id,
                "total_quotes": comparison_doc.get("total_pdfs", 0),
                "summary": comparison_doc.get("summary", {}),
                "key_differences": comparison_doc.get("key_differences", {}),
                "side_by_side": comparison_doc.get("side_by_side", {}),
                "data_table": comparison_doc.get("data_table", {}),
                "analytics": comparison_doc.get("analytics", {}),
                "provider_cards": comparison_doc.get("provider_cards", []),
            }

        # Generate PDF
        pdf_buffer = pdf_generator_service.generate_comparison_pdf(
            comparison_data=comparison_data, comparison_id=comparison_id
        )

        # Get PDF bytes
        pdf_bytes = pdf_buffer.getvalue()

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"HAKEM_AI_Comparison_{comparison_id}_{timestamp}.pdf"

        logger.info(
            f"✅ PDF generated successfully: {filename} ({len(pdf_bytes)} bytes)"
        )

        # Return PDF as download
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(pdf_bytes)),
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error generating PDF: {e}")
        import traceback

        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")


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
            ],
        }
    )
