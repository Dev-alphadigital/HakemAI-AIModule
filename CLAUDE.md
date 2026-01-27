# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## HakemAI AI Module - FastAPI Insurance Quote Processing Engine

This is the **AI Module** of the HakemAI platform - a FastAPI-based microservice that handles PDF extraction, AI-powered quote analysis, and insurance provider ranking.

## SYSTEM RULES

**Agent Execution Workflow:**
- **Planner runs FIRST** - Must create implementation plan before any code changes
- **Builder runs ONLY after Planner** - Implements code based on approved plan
- **Critic runs ONLY after Builder** - Reviews completed implementation
- **No agent may violate its role** - Each agent must stay within its designated responsibilities
- **If an agent violates rules, stop and correct** - Halt execution and return to proper workflow order

### Architecture Context

This module is part of a 3-service microservices architecture:
- **HakemAI-Frontend** (Next.js 15) - UI on port 3000
- **HakemAi-Backend** (NestJS) - Auth & user management on port 5000
- **HakemAI-AIModule** (FastAPI) - This module on port 8000

**Shared Database:** All services share a MongoDB database (`hakemAI`). Backend writes user/subscription data, AI Module reads it. AI Module writes comparison/extraction data.

### Tech Stack

- **Framework:** FastAPI 0.115.6 with Uvicorn 0.32.1
- **AI:** OpenAI GPT-4 (gpt-4o-mini) for quote extraction
- **Database:** MongoDB (Motor 3.6.0 async + PyMongo 4.9.1 sync)
- **PDF:** PyMuPDF 1.24.14 (extraction) + ReportLab 4.2.5 (generation)
- **Validation:** Pydantic 2.9.2 with strict type enforcement
- **Web Scraping:** BeautifulSoup 4.12, Requests, lxml (for logos)

### Running the Application

```bash
# Activate virtual environment
venv\Scripts\activate  # Windows
source venv/bin/activate  # macOS/Linux

# Start development server with auto-reload
python main.py

# Production server with Gunicorn
gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8000
```

**API Documentation:** http://localhost:8000/docs (Swagger UI)

### Project Structure

```
app/
├── api/
│   └── routes.py              # All HTTP endpoints (compare, retrieve, management)
├── core/
│   ├── config.py              # Settings & environment variables
│   └── openai_client.py       # OpenAI API wrapper
├── models/
│   ├── scheme.py              # Main Pydantic models (ExtractedQuoteData, RankedQuote)
│   ├── quote_model.py         # Re-exports from scheme.py
│   ├── analytics_model.py     # Analytics response models
│   └── document_model.py      # Document storage models
├── services/
│   ├── ai_parser.py           # 6-stage AI extraction pipeline (v9.0)
│   ├── ai_ranker.py           # 70/30 ranking algorithm
│   ├── comparison_service.py  # Side-by-side comparison logic
│   ├── pdf_extractor.py       # PyMuPDF text extraction
│   ├── pdf_generator_service.py  # ReportLab report generation
│   ├── mongodb_service_enhanced.py  # Individual PDF + comparison storage
│   ├── user_subscription_service.py  # Subscription validation
│   ├── user_documents_service.py     # Document metadata storage
│   ├── hakim_score_service.py        # Provider reputation scores
│   ├── activity_logs_service.py      # Activity tracking
│   └── progress_tracker.py           # Real-time progress updates
└── utils/
    ├── helpers.py             # Currency parsing, validation
    ├── file_utils.py          # File operations
    └── response_formatter.py  # API response formatting
```

### Key Architecture Patterns

#### 1. AI Parser Pipeline (ai_parser.py v9.0)

6-stage extraction with fallback strategies:

1. **Stage 1-3:** AI extraction with structured prompts
2. **Stage 3.5:** Validation + pattern-based fallback
3. **Stage 4:** Subjectivities & requirements
4. **Stage 5:** Calculations + **VAT Detection** (comprehensive v9.0)
5. **Stage 6:** Analysis and scoring preparation

**Critical VAT Detection Rules (v9.0):**
- **P1 (VAT-Inclusive):** Premium includes VAT → `vat_percentage=None`, `vat_amount=None`
- **P2 (VAT-Exclusive):** VAT will be added → `vat_percentage=15.0` (Saudi standard)
- **P3→P2 (No VAT Mention):** Default to P2 with 15% + warning flag
- **P5 (Rejected):** Zero VAT explicitly stated → Raise `VatPolicyViolation`
- **P6 (Rejected):** Non-standard VAT rate (not 15%) → Raise `VatPolicyViolation`

Pattern-first detection: 120+ patterns checked exhaustively before defaulting.

#### 2. 70/30 Ranking Formula (ai_ranker.py)

**70% Quote Factors:**
- Premium (normalized by sum insured)
- Coverage breadth
- Deductibles
- Exclusions
- Extensions
- Policy benefits

**30% Hakim Score:**
- Provider reputation (0-100)
- Stored in `hakim_scores` collection
- Admin-managed via Backend

#### 3. MongoDB Data Storage

**Collections Used:**
- `users` - Read-only: subscription validation
- `comparisons` - Write: Final comparison results
- `individual_pdf_extractions` - Write: Each PDF stored separately
- `user_documents` - Write: Document metadata
- `hakim_scores` - Read: Provider reputation
- `activity_logs` - Write: Activity tracking
- `progress_updates` - Write: Real-time progress

**Storage Pattern:**
```
One comparison → Multiple individual_pdf_extractions
comparison_id links all PDFs from same upload
No data mixing between PDFs
```

#### 4. User Authentication Flow

Frontend sends `X-User-Id` header → AI Module validates subscription → Processes request

**Subscription Validation:**
```python
from app.services.user_subscription_service import user_subscription_service
is_valid, message = await user_subscription_service.validate_user_subscription(user_id)
if not is_valid:
    raise HTTPException(status_code=403, detail=message)
```

### Critical Development Patterns

#### Error Handling for VAT Violations

```python
from app.services.ai_parser import ai_parser, VatPolicyViolation

try:
    extracted_data = await ai_parser.parse_insurance_quote(text, filename)
except VatPolicyViolation as e:
    # Document violates VAT policy - do not proceed to ranking
    logger.error(f"VAT Policy Violation: {e.vat_class} - {e.reason}")
    # Return error to user, do not store in comparisons
```

#### MongoDB Connection Management

Services use singleton pattern with lifespan management:

```python
# In main.py lifespan
await enhanced_mongodb_service.connect()  # Startup
await enhanced_mongodb_service.disconnect()  # Shutdown
```

Never create new connections in route handlers - use existing services.

#### Progress Tracking for Long Operations

```python
from app.services.progress_tracker import progress_tracker

# Initialize progress
tracker_id = await progress_tracker.initialize(total_files, user_id)

# Update during processing
await progress_tracker.update(
    tracker_id=tracker_id,
    completed_files=i + 1,
    status="processing",
    current_file=filename
)

# Complete
await progress_tracker.complete(tracker_id, comparison_id)
```

Frontend polls progress endpoint for real-time updates.

### Environment Variables (.env)

**Required:**
```bash
OPENAI_API_KEY=sk-proj-...           # OpenAI API key (REQUIRED)
MONGODB_URL=mongodb://localhost:27017
MONGODB_DATABASE=hakemAI
AI_MODEL=gpt-4o-mini
PORT=8000
```

**Optional:**
```bash
DEBUG=false                          # Enable debug logging
ALLOWED_ORIGINS=http://localhost:3000  # CORS origins
```

**Note:** Config tries `MONGO_URI` first (Backend convention), falls back to `MONGODB_URL`.

### Common Development Tasks

#### Run Development Server
```bash
python main.py  # Auto-reload enabled
```

#### Test API Endpoints
```bash
# Health check
curl http://localhost:8000/api/health

# Interactive docs
open http://localhost:8000/docs
```

#### Update VAT Detection Patterns

Edit `app/services/ai_parser.py`:
- `VAT_INCLUSIVE_PATTERNS` (lines 94+)
- `VAT_EXCLUSIVE_PATTERNS` (lines 130+)
- Pattern matching is case-insensitive with regex support

#### Add New Hakim Scores

Use admin script:
```bash
python scripts/initialize_hakim_scores.py
```

Or via Backend admin API (Frontend admin dashboard).

#### Debug MongoDB Queries

```bash
mongosh
use hakemAI
db.comparisons.find().pretty()
db.individual_pdf_extractions.find({comparison_id: "cmp_..."})
```

### Testing Patterns

#### Manual PDF Testing

1. Start server: `python main.py`
2. Upload via Swagger UI: http://localhost:8000/docs
3. Use `POST /api/compare-quotes` with PDF files
4. Check `X-User-Id` header requirement

#### Validate Subscription Before Testing

Ensure user exists in MongoDB `users` collection with active subscription:
```javascript
db.users.findOne({_id: ObjectId("...")})
// Check: subscription_status: "ACTIVE"
```

### Important Constraints

1. **No Direct Service-to-Service HTTP:** Backend and AI Module communicate via MongoDB only
2. **Shared Database Schema:** Changes to `users`, `hakim_scores` collections affect Backend
3. **Pydantic Strict Mode:** All models use strict validation - no auto-coercion
4. **OpenAI Rate Limits:** gpt-4o-mini has rate limits - handle `RateLimitError`
5. **VAT Policy Enforcement:** P5/P6 classifications MUST reject documents - no ranking allowed
6. **Subscription Check First:** Always validate user subscription before processing PDFs
7. **Individual PDF Storage:** Store each extraction separately with `comparison_id` link

### Recent Major Changes

**v9.0 (2026-01-27):** Comprehensive VAT detection overhaul
- Pattern-first approach with 120+ patterns
- Graceful P3→P2 fallback with 15% default
- Full document search (no character limits)
- Maintained strict P5/P6 rejection policy

**v3.2:** Individual PDF storage
- Each PDF extraction stored separately in `individual_pdf_extractions`
- Final comparison in `comparisons` collection
- No data mixing between uploads

**v3.1:** Simplified orchestration
- Single endpoint auto-populates all sections
- No manual section generation needed

### Troubleshooting

**OpenAI API Errors:**
- Check `.env` has valid `OPENAI_API_KEY`
- Verify account credits: https://platform.openai.com/usage
- Rate limit errors: Implement exponential backoff

**MongoDB Connection Fails:**
- Check MongoDB is running: `mongosh`
- Verify `MONGODB_URL` in `.env`
- Check firewall/network if using Atlas

**VAT Detection Issues:**
- Check `app.log` for pattern match results
- Search for "VAT CLASSIFICATION" in logs
- Review document text extraction quality

**Port Already in Use:**
- Change `PORT` in `.env`
- Kill process: `netstat -ano | findstr :8000` (Windows)
