

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import logging
import sys
import os
import io
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime

from app.core.config import settings
from app.api.routes import router
from app.services.mongodb_service_enhanced import enhanced_mongodb_service
from app.services.progress_tracker import start_cleanup_task
from app.services.user_subscription_service import user_subscription_service
from app.services.user_documents_service import user_documents_service


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

class UTF8StreamHandler(logging.StreamHandler):
    """Custom handler with UTF-8 encoding for Windows compatibility."""
    def __init__(self):
        if sys.platform == 'win32':
            sys.stdout = io.TextIOWrapper(
                sys.stdout.buffer,
                encoding='utf-8',
                errors='replace',
                line_buffering=True
            )
        super().__init__(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        UTF8StreamHandler(),
        logging.FileHandler('app.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


# ============================================================================
# STARTUP/SHUTDOWN LIFECYCLE
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager.
    Handles startup and shutdown tasks.
    """
    # Startup
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"  INSURANCE QUOTE COMPARISON API v3.1")
    logger.info(f"  Production-Ready Complete Integration")
    logger.info("=" * 80)
    logger.info(f"  AI Model: {settings.OPENAI_MODEL}")
    logger.info(f"  Upload Directory: {settings.UPLOAD_DIR}")
    logger.info(f"  Max File Size: {settings.MAX_FILE_SIZE_MB}MB")
    logger.info("=" * 80)
    
    # Ensure directories exist
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    logger.info(f"  ✅ Upload directory ready: {settings.UPLOAD_DIR}")
    
    os.makedirs("logs", exist_ok=True)
    logger.info(f"  ✅ Logs directory ready: logs/")
    
    # Initialize MongoDB connection
    try:
        await enhanced_mongodb_service.connect()
        logger.info(f"  ✅ Enhanced MongoDB connected: {settings.MONGODB_URL}")
    except Exception as e:
        logger.error(f"  ❌ MongoDB connection failed: {e}")
        logger.warning("  ⚠️  Application will continue without MongoDB")
    
    # Initialize User Subscription Service (connects to same MongoDB)
    try:
        await user_subscription_service.connect()
        logger.info(f"  ✅ User Subscription Service connected")
    except Exception as e:
        logger.error(f"  ❌ User Subscription Service connection failed: {e}")
        logger.warning("  ⚠️  Application will continue without user subscription checks")
    
    # Initialize User Documents Service (connects to same MongoDB)
    try:
        await user_documents_service.connect()
        logger.info(f"  ✅ User Documents Service connected")
    except Exception as e:
        logger.error(f"  ❌ User Documents Service connection failed: {e}")
        logger.warning("  ⚠️  Application will continue without user documents storage")
    
    # Start progress tracker cleanup task
    try:
        asyncio.create_task(start_cleanup_task())
        logger.info("  ✅ Progress tracker cleanup task started")
    except Exception as e:
        logger.warning(f"  ⚠️  Could not start progress cleanup: {e}")
    
    logger.info("")
    logger.info("  🎯 INTEGRATED COMPONENTS:")
    logger.info("     • AI Parser v6.0: 6-stage comprehensive extraction")
    logger.info("       - Proper entity identification (insurer vs insured)")
    logger.info("       - Separates warranties from extensions")
    logger.info("       - Extracts subjectivities & operational details")
    logger.info("       - Correct deductible tier detection")
    logger.info("       - Full benefits extraction (30+, not 4)")
    logger.info("")
    logger.info("     • AI Ranker v2.0: Complete comparison engine")
    logger.info("       - Generates ALL sections comprehensively")
    logger.info("       - Identifies unique items per provider")
    logger.info("       - Transparent scoring methodology")
    logger.info("       - Uses full _extended_data from parser")
    logger.info("")
    logger.info("     • Routes v3.1: Simplified orchestration")
    logger.info("       - Single endpoint auto-populates everything")
    logger.info("       - No manual section generation")
    logger.info("       - Complete MongoDB integration")
    logger.info("")
    logger.info("  🔧 CRITICAL FIXES IN v3.1:")
    logger.info("     ✅ Benefits count: 30+ (was 4)")
    logger.info("     ✅ Warranty categorization: Proper separation")
    logger.info("     ✅ Subjectivities: Now included")
    logger.info("     ✅ Deductibles: Correct tier (SR 1M, not SR 50K)")
    logger.info("     ✅ Operational details: Validity, payment, brokerage")
    logger.info("     ✅ Scoring: Transparent methodology with breakdown")
    logger.info("")
    logger.info("  📡 AVAILABLE ENDPOINTS:")
    logger.info("     Main:")
    logger.info("       POST /api/compare-quotes           Upload PDFs, get everything")
    logger.info("")
    logger.info("     Retrieval:")
    logger.info("       GET  /api/comparisons/{id}         Complete comparison")
    logger.info("       POST /api/summary                  Rankings & analysis")
    logger.info("       POST /api/key-differences          Unique items per provider")
    logger.info("       POST /api/side-by-side             Top providers comparison")
    logger.info("       POST /api/data-table               Sortable data grid")
    logger.info("       POST /api/analytics                Charts & statistics")
    logger.info("       POST /api/provider-cards           UI-ready summaries")
    logger.info("")
    logger.info("     Management:")
    logger.info("       GET  /api/comparisons/recent       Recent comparisons")
    logger.info("       GET  /api/comparisons/search       Search by provider")
    logger.info("       DELETE /api/comparisons/{id}       Delete comparison")
    logger.info("")
    logger.info("     Documents (MongoDB):")
    logger.info("       GET  /api/documents                All documents")
    logger.info("       GET  /api/documents/{id}           Specific document")
    logger.info("       GET  /api/documents/search/{q}     Search documents")
    logger.info("       DELETE /api/documents/{id}         Delete document")
    logger.info("")
    logger.info("     Diagnostics & Health:")
    logger.info("       GET  /api/health                   System health")
    logger.info("       GET  /api/status                   Detailed status")
    logger.info("       GET  /api/diagnostics/comparison/{id}  Comparison diagnostics")
    logger.info("")
    logger.info("  📊 AUTO-GENERATED SECTIONS:")
    logger.info("     ✓ Summary with rankings & detailed analysis")
    logger.info("     ✓ Key Differences (unique warranties, exclusions)")
    logger.info("     ✓ Side-by-Side (top providers comparison matrix)")
    logger.info("     ✓ Data Table (sortable, filterable)")
    logger.info("     ✓ Analytics (premium charts, score charts, features)")
    logger.info("     ✓ Provider Cards (UI-ready summaries)")
    logger.info("     ✓ Extracted Quotes (full data with all benefits)")
    logger.info("")
    logger.info("  🎯 WORKFLOW:")
    logger.info("     1. Upload 1-10 PDFs → POST /api/compare-quotes")
    logger.info("     2. Receive comparison_id + complete data")
    logger.info("     3. Retrieve sections using comparison_id")
    logger.info("     4. No re-upload needed!")
    logger.info("")
    logger.info("=" * 80)
    logger.info("  ✅ Application started successfully!")
    logger.info(f"  📚 API Docs: http://localhost:{os.getenv('PORT', '8000')}/docs")
    logger.info(f"  📖 ReDoc: http://localhost:{os.getenv('PORT', '8000')}/redoc")
    logger.info("=" * 80)
    logger.info("")
    
    yield
    
    # Shutdown
    logger.info("")
    logger.info("=" * 80)
    logger.info("  🛑 Shutting down application...")
    
    # Disconnect from MongoDB
    try:
        await enhanced_mongodb_service.disconnect()
        logger.info("  ✅ Enhanced MongoDB connection closed")
    except Exception as e:
        logger.warning(f"  ⚠️  MongoDB disconnect: {str(e)}")
    
    # Disconnect User Subscription Service
    try:
        await user_subscription_service.disconnect()
        logger.info("  ✅ User Subscription Service connection closed")
    except Exception as e:
        logger.warning(f"  ⚠️  User Subscription Service disconnect: {str(e)}")
    
    # Disconnect User Documents Service
    try:
        await user_documents_service.disconnect()
        logger.info("  ✅ User Documents Service connection closed")
    except Exception as e:
        logger.warning(f"  ⚠️  User Documents Service disconnect: {str(e)}")
    
    # Cleanup temporary files
    try:
        cleanup_count = 0
        for file in Path(settings.UPLOAD_DIR).glob("*"):
            if file.is_file():
                file.unlink()
                cleanup_count += 1
        logger.info(f"  ✅ Cleaned up {cleanup_count} temporary files")
    except Exception as e:
        logger.warning(f"  ⚠️  Cleanup: {str(e)}")
    
    logger.info("  ✅ Application stopped successfully")
    logger.info("=" * 80)
    logger.info("")


# ============================================================================
# APPLICATION INITIALIZATION
# ============================================================================

app = FastAPI(
    title=settings.API_TITLE,
    version="3.1.0",
    description=f"""{settings.API_DESCRIPTION}



## 📖 Documentation

- **Interactive API Docs**: `/docs` (Swagger UI)
- **Alternative Docs**: `/redoc` (ReDoc)
- **Health Check**: `/api/health`
""",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)


# ============================================================================
# MIDDLEWARE CONFIGURATION
# ============================================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:8080",
        "*",  # Allow all in development - CHANGE IN PRODUCTION
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests with timing."""
    start_time = datetime.now()
    
    logger.info(f"📥 {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"📤 {request.method} {request.url.path} - {response.status_code} - {duration:.2f}s")
        
        return response
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ {request.method} {request.url.path} - Error: {str(e)} - {duration:.2f}s")
        raise


# ============================================================================
# ROUTE INCLUSION
# ============================================================================

app.include_router(router)


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Handle 404 errors."""
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "detail": f"Resource not found: {request.url.path}",
            "available_endpoints": [
                "/api/compare-quotes",
                "/api/comparison/{comparison_id}/pdf",
                "/api/comparisons/{id}",
                "/api/summary",
                "/api/key-differences",
                "/api/side-by-side",
                "/api/analytics",
                "/api/my-documents",
                "/api/my-documents/{document_id}/download",
                "/api/health",
                "/docs",
                "/redoc"
            ]
        }
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    """Handle 500 errors."""
    logger.error(f"Internal error on {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": "An unexpected error occurred",
            "request_id": id(request),
            "path": str(request.url.path),
            "support": "Check /api/health for system status"
        }
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler."""
    logger.error(f"Unhandled exception on {request.url.path}: {str(exc)}", exc_info=True)
    
    detail = str(exc) if settings.DEBUG else "An error occurred"
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": detail,
            "type": type(exc).__name__,
            "path": str(request.url.path)
        }
    )


# ============================================================================
# ROOT ENDPOINTS
# ============================================================================

@app.get("/")
async def read_root():
    """Root endpoint - API welcome and overview."""
    return {
        "message": "Insurance Quote Comparison API v3.1",
        "version": "3.1.0",
        "status": "operational",
        "documentation": "/docs",
        "redoc": "/redoc",
        "timestamp": datetime.now().isoformat(),
        
        "integrated_components": {
            "parser": "v6.0 (6-stage comprehensive extraction)",
            "ranker": "v2.0 (complete comparison engine)",
            "routes": "v3.1 (simplified orchestration)",
            "storage": "MongoDB (persistent)"
        },
        
        "critical_fixes_v3_1": {
            "benefits_extraction": "30+ benefits (was 4)",
            "warranty_categorization": "Separated from extensions",
            "subjectivities": "Now included",
            "deductible_tiers": "Correct detection (SR 1M for >500M)",
            "operational_details": "Validity, payment terms, brokerage",
            "scoring_transparency": "Full methodology breakdown",
            "data_completeness": "Uses full _extended_data"
        },
        
        "features": {
            "auto_population": True,
            "multi_pdf_support": "1-10 PDFs per request",
            "ai_powered": f"{settings.OPENAI_MODEL}",
            "comprehensive_sections": [
                "Summary with rankings",
                "Key differences (unique items)",
                "Side-by-side comparison",
                "Data table (sortable)",
                "Analytics & charts",
                "Provider cards"
            ],
            "duplicate_detection": True,
            "rate_validation": "‰, %, bp support",
            "mongodb_persistence": True
        },
        
        "workflow": {
            "step_1": "Upload PDFs → POST /api/compare-quotes",
            "step_2": "Receive comparison_id + complete data",
            "step_3": "Retrieve sections using comparison_id",
            "step_4": "No re-upload needed for different views"
        },
        
        "supported_policies": [
            "Property Insurance",
            "Fire Insurance",
            "All Risks",
            "Liability (CGL)",
            "Medical Malpractice",
            "Motor Insurance",
            "Business Interruption"
        ],
        
        "quick_start": {
            "1_upload": "POST /api/compare-quotes with PDF files",
            "2_get_data": "Response includes comparison_id + all sections",
            "3_retrieve": "POST /api/side-by-side with comparison_id",
            "4_analyze": "POST /api/analytics with comparison_id"
        },
        
        "limitations": {
            "max_files": 10,
            "min_files": 1,
            "max_file_size_mb": settings.MAX_FILE_SIZE_MB,
            "formats": ["PDF"],
            "retention_hours": 24
        },
        
        "endpoints": {
            "main": "/api/compare-quotes",
            "health": "/api/health",
            "diagnostics": "/api/diagnostics/comparison/{id}",
            "docs": "/docs"
        }
    }


@app.get("/api")
async def api_root():
    """API root with endpoint reference."""
    return {
        "message": "Insurance Quote Comparison API v3.1",
        "version": "3.1.0",
        "documentation": "/docs",
        
        "main_endpoint": {
            "path": "/api/compare-quotes",
            "method": "POST",
            "description": "Upload PDFs, get complete comparison with all sections",
            "accepts": "multipart/form-data (PDF files)",
            "returns": "Complete comparison + comparison_id"
        },
        
        "retrieval_endpoints": {
            "complete": "GET /api/comparisons/{id}",
            "summary": "POST /api/summary (comparison_id required)",
            "differences": "POST /api/key-differences (comparison_id required)",
            "side_by_side": "POST /api/side-by-side (comparison_id required)",
            "data_table": "POST /api/data-table (comparison_id required)",
            "analytics": "POST /api/analytics (comparison_id required)",
            "cards": "POST /api/provider-cards (comparison_id required)"
        },
        
        "management_endpoints": {
            "recent": "GET /api/comparisons/recent",
            "search": "GET /api/comparisons/search?query={text}",
            "delete": "DELETE /api/comparisons/{id}"
        },
        
        "document_endpoints": {
            "all": "GET /api/documents",
            "get": "GET /api/documents/{id}",
            "search": "GET /api/documents/search/{query}",
            "delete": "DELETE /api/documents/{id}"
        },
        
        "diagnostic_endpoints": {
            "health": "GET /api/health",
            "status": "GET /api/status",
            "comparison_diagnostics": "GET /api/diagnostics/comparison/{id}"
        }
    }


@app.get("/health")
async def health_check_root():
    """Root health check."""
    return {
        "status": "healthy",
        "version": "3.1.0",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "parser": "v6.0",
            "ranker": "v2.0",
            "routes": "v3.1"
        },
        "api_documentation": "/docs"
    }


# ============================================================================
# DEVELOPMENT SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", "8000"))
    
    logger.info(f"\n🚀 Starting development server on port {port}...")
    logger.info(f"📚 API Docs: http://localhost:{port}/docs")
    logger.info(f"📖 ReDoc: http://localhost:{port}/redoc")
    logger.info(f"🏠 Root: http://localhost:{port}/")
    logger.info("")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info",
        access_log=True,
        reload_dirs=["app"],
        reload_excludes=["*.log", "uploads/*", "logs/*"]
    )