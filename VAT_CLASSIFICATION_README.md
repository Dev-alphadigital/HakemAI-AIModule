# VAT Classification System

## Overview

The HakemAI AI Module uses a 6-tier classification system (P1-P6) to categorize insurance quote PDFs based on how VAT (Value Added Tax) is presented. This ensures fair premium comparisons across different insurance providers.

**Version**: v9.0 (Pattern-First Detection)
**Saudi VAT Standard**: 15%

---

## Classification Categories

### ✅ P1 - VAT Inclusive (ACCEPTED)

**Definition**: Premium amount already includes VAT. No separate VAT breakdown provided.

**Document Indicators**:
- "Total Premium with VAT included"
- "Premium inclusive of VAT"
- "VAT inclusive"
- "Including VAT"
- "Incl. VAT"

**Behavior**:
- `vat_percentage`: `null`
- `vat_amount`: `null`
- Premium used as-is for comparison (no VAT calculation needed)

**Example**:
```
Total Premium: SAR 5,000 (with VAT included)
→ Classification: P1
→ Comparison Premium: SAR 5,000
```

---

### ✅ P2 - VAT Exclusive (ACCEPTED)

**Definition**: Premium excludes VAT. VAT will be added separately at 15%.

**Document Indicators**:
- "VAT 15%" / "15% VAT"
- "Plus VAT as applicable"
- "Premium Subject to VAT"
- "VAT applicable"
- "VAT exclusive"
- "Taxes charged at billing"
- "VAT will be added"
- "VAT: SAR 750"
- "SAR 5,000 + 15% VAT"

**Behavior**:
- `vat_percentage`: `15.0`
- `vat_amount`: Calculated or extracted from document
- Premium normalized for fair comparison (VAT-exclusive basis)

**Example**:
```
Premium: SAR 5,000
VAT (15%): SAR 750
Total: SAR 5,750
→ Classification: P2
→ Comparison Premium: SAR 5,000 (VAT-exclusive for fair comparison)
```

---

### ❌ P3 - No VAT Mention (OBSOLETE in v9.0)

**Definition**: ~~No VAT information found in document~~

**Status**: **No longer used as rejection reason**

**v9.0 Behavior**: Documents with no explicit VAT mention default to **P2** with:
- `vat_percentage`: `15.0` (Saudi standard)
- `vat_amount`: Calculated as 15% of premium
- `confidence`: `low`
- `requires_verification`: `true`
- `warning`: "No explicit VAT patterns found. Defaulting to P2 with Saudi standard 15% VAT."

This graceful fallback allows quotes to proceed while flagging them for human review, aligning with Saudi government policy that mandates 15% VAT for insurance.

---

### ❌ P4 - Total Only, No Breakdown (OBSOLETE in v9.0)

**Definition**: ~~Shows "Total including VAT" but provides no VAT details~~

**Status**: **No longer used as rejection reason**

**v9.0 Behavior**: Documents showing totals without VAT breakdown are now classified as:
- **P1** if "inclusive" language detected
- **P2** with medium confidence if unclear

This prevents rejection of legitimate quotes that mention totals without explicit breakdowns.

---

### ❌ P5 - Zero VAT (REJECTED)

**Definition**: Document explicitly states VAT is 0%.

**Document Indicators**:
- "VAT: 0%"
- "VAT 0"
- "VAT: 0.0%"
- "0% VAT applicable"

**Behavior**:
- **Quote is REJECTED**
- Raises `VatPolicyViolation` error
- Reason: Invalid for Saudi Arabia where 15% VAT is mandatory

**Example**:
```
Premium: SAR 5,000
VAT (0%): SAR 0
→ Classification: P5
→ Result: REJECTED ❌
```

---

### ❌ P6 - Non-Standard VAT Rate (REJECTED)

**Definition**: Document shows VAT rate other than Saudi standard 15%.

**Document Indicators**:
- "VAT 69%"
- "VAT: 20%"
- "SAR 5,000 + 69% VAT"
- Any rate ≠ 15%

**Behavior**:
- **Quote is REJECTED**
- Raises `VatPolicyViolation` error
- Reason: Non-standard rate not allowed in Saudi Arabia

**Example**:
```
Premium: SAR 5,000
VAT (69%): SAR 3,450
→ Classification: P6
→ Result: REJECTED ❌
```

---

## Detection Methodology

### Pattern-First Approach (v9.0)

The system uses **120+ regex patterns** to search the full document text before trusting AI-extracted values. This prevents VAT hallucination and ensures accurate classification.

#### Detection Flow:

```
1. Extract full text from PDF
   ↓
2. Check for P5/P6 patterns (zero VAT, non-standard rates)
   → If found: REJECT immediately
   ↓
3. Check VAT_EXCLUSIVE patterns (60 patterns)
   → If found: Classify as P2
   ↓
4. Check VAT_INCLUSIVE patterns (25 patterns)
   → If found: Classify as P1
   ↓
5. Check generic VAT_MENTIONED patterns (5 patterns)
   → If found: Classify as P2 with medium confidence
   ↓
6. No patterns found (fallback)
   → Classify as P2 with low confidence + warning
```

#### Pattern Categories:

**VAT_EXCLUSIVE_PATTERNS** (60 patterns):
- Direct exclusive keywords: "VAT exclusive", "exclusive of VAT"
- "Plus VAT" statements: "plus VAT", "+ VAT", "plus VAT as applicable"
- "Applicable" statements: "VAT as applicable", "VAT applicable"
- "Subject to" statements: "subject to VAT", "premium subject to VAT"
- Standard percentages: "VAT 15%", "15% VAT", "VAT @ 15%"
- VAT amounts: "VAT: SAR 750", "SAR 750 VAT"
- Formulas: "SAR 5,000 + 15% VAT", "premium + VAT"
- Administrative: "VAT charged at billing", "taxes as per law"

**VAT_INCLUSIVE_PATTERNS** (25 patterns):
- Direct inclusive keywords: "VAT inclusive", "inclusive of VAT"
- "Included" statements: "VAT included", "with VAT included"
- Total statements: "Total Premium with VAT included"
- Combined: "premium inclusive of VAT", "incl. VAT"

**VAT_MENTIONED_PATTERNS** (5 patterns):
- Generic mentions: "VAT", "value added tax", "V.A.T."

---

## Output Metadata

Each classification includes detailed metadata for transparency and verification:

```json
{
  "vat_class": "P1" | "P2",
  "vat_percentage": 15.0 | null,
  "vat_amount": 750.0 | null,
  "vat_detection_method": "pattern_first_v9",

  "vat_classification": {
    "class": "P2",
    "detection_method": "pattern_first_v9",
    "pattern_matched": "plus\\s+vat\\s+as\\s+applicable",
    "confidence": "high" | "medium" | "low",
    "is_assumed": false,
    "warning": null,
    "requires_verification": false
  }
}
```

### Confidence Levels:

- **High**: Explicit VAT pattern matched (e.g., "VAT 15%", "VAT inclusive")
- **Medium**: Generic VAT mention but no explicit inclusive/exclusive statement
- **Low**: No VAT patterns found, using Saudi 15% default

---

## Comparison Logic

### Fair Premium Comparison:

To ensure apples-to-apples comparison between providers:

1. **P1 documents**: Premium used as-is (VAT already included)
2. **P2 documents**: Premium normalized to VAT-exclusive basis

**Example Comparison**:

| Provider | Classification | Stated Premium | VAT | Total | Comparison Premium |
|----------|---------------|----------------|-----|-------|-------------------|
| Provider A | P1 (inclusive) | SAR 5,750 | N/A | SAR 5,750 | **SAR 5,750** |
| Provider B | P2 (exclusive) | SAR 5,000 | SAR 750 | SAR 5,750 | **SAR 5,750** |

Both providers quote the same final amount (SAR 5,750), ensuring fair comparison.

---

## Error Handling

### Rejection Cases:

**P5 Rejection Example**:
```json
{
  "error": "VatPolicyViolation",
  "message": "Document violates VAT policy: P5 (Zero VAT detected). Saudi Arabia requires 15% VAT for insurance.",
  "vat_class": "P5"
}
```

**P6 Rejection Example**:
```json
{
  "error": "VatPolicyViolation",
  "message": "Document violates VAT policy: P6 (Non-standard VAT rate: 69%). Only 15% allowed.",
  "vat_class": "P6",
  "detected_rate": 69.0
}
```

### Warning Cases:

Documents classified with low confidence are flagged but **not rejected**:

```json
{
  "vat_class": "P2",
  "vat_percentage": 15.0,
  "vat_classification": {
    "confidence": "low",
    "requires_verification": true,
    "warning": "No explicit VAT patterns found. Defaulting to P2 with Saudi standard 15% VAT."
  }
}
```

---

## Key Features

✅ **Comprehensive Pattern Matching**: 120+ patterns cover all VAT mention variations
✅ **Hallucination Prevention**: Pattern-first validation ensures accuracy
✅ **Full Document Search**: No character limits, searches entire PDF
✅ **Graceful Fallback**: No blanket rejections, provides warnings for review
✅ **Saudi Compliance**: Respects 15% VAT standard for government policies
✅ **Confidence Tracking**: Transparent classification with verification flags

---

## Technical Implementation

**File**: `app/services/ai_parser.py`
**Function**: `_comprehensive_vat_detection()`
**Lines**: 1303-1523 (detection), 2558-2671 (integration)

**Dependencies**:
- `re` (regex pattern matching)
- OpenAI GPT-4 (text extraction)
- PyMuPDF (PDF text extraction)

---

## Version History

- **v9.0** (2026-01-27): Comprehensive pattern-first detection, graceful fallback, removed P3/P4 rejections
- **v8.0**: Enhanced pattern coverage, expanded search ranges
- **v7.0**: Added P1 null enforcement, hallucination prevention
- **v1.0-v6.0**: Initial signal-based detection (deprecated)

---

## Support

For issues or questions regarding VAT classification:
- Review logs for `vat_classification` metadata
- Check `requires_verification` flag for uncertain cases
- Monitor `confidence` levels for quality assessment
- Report edge cases for pattern enhancement

**Last Updated**: 2026-01-27
**Current Version**: v9.0
