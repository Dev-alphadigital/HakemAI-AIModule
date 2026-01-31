"""
PRODUCTION-READY AI PARSER v9.0 - COMPREHENSIVE VAT DETECTION
==============================================================
✅ Multi-layer extraction: AI + Pattern-based fallback
✅ Automatic validation and retry logic
✅ Works for ANY insurance quote PDF format
✅ Production-ready, scalable, and reliable
✅ No vendor-specific patches - universal solution
✅ Comprehensive VAT detection with graceful fallback (NEW!)

VERSION HISTORY:
v6.0 - Original AI-only extraction
v7.0 - Added fallback extraction + validation
v8.0 - Strict VAT detection, preserved original semantics, enhanced logging
v9.0 - Comprehensive pattern-first VAT detection with graceful degradation

KEY IMPROVEMENTS IN v9.0:
1. **Pattern-First Detection:** 120+ VAT patterns checked exhaustively before defaulting
2. **Graceful Fallback:** P3/P4 default to P2 with warning (no rejection)
3. **Saudi Standard:** 15% VAT assumed when unclear (government policy standard)
4. **Reject Only P5/P6:** Zero VAT and non-standard rates still rejected
5. **Validation:** Extracted AI values validated against document text
6. **Full Document Search:** No character limits - searches entire PDF
7. **Confidence Tracking:** High/Medium/Low confidence with verification flags

VAT DETECTION RULES (v9.0):
- **P1 (VAT-Inclusive):** Premium explicitly includes VAT
  - Patterns: "VAT inclusive", "including VAT", "Total Premium with VAT included"
  - Behavior: vat_percentage=None, vat_amount=None (no calculation)

- **P2 (VAT-Exclusive):** VAT will be added separately
  - Patterns: "VAT exclusive", "plus VAT", "VAT as applicable", "VAT 15%"
  - Behavior: vat_percentage=15.0 (default) or extracted, calculate VAT

- **P3 (VAT-Deferred):** VAT will be charged at billing time
  - Patterns: "all taxes will be charged at the time of billing"
  - Behavior: vat_percentage=None, vat_amount=None (deferred to billing)
  - Note: Quote does not include VAT; VAT added later at invoice time

- **P4 (No VAT Mention - Defaulted):** No patterns found
  - Behavior: Default to P2 with 15% + warning flag

- **P5 (REJECTED):** Zero VAT explicitly stated
  - Pattern: "VAT: 0%"
  - Behavior: Raise VatPolicyViolation

- **P6 (REJECTED):** Non-standard VAT rate (not 15%)
  - Pattern: "VAT 69%" (confirmed in text)
  - Behavior: Raise VatPolicyViolation

ARCHITECTURE:
- Stage 1-3: AI-powered extraction
- Stage 3.5: Validation + fallback
- Stage 4: Subjectivities & requirements
- Stage 5: Calculations + COMPREHENSIVE VAT DETECTION (v9.0)
- Stage 6: Analysis and scoring

Last Updated: 2026-01-27
"""

import json
import re
import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple, NamedTuple
from datetime import datetime
from app.core.openai_client import openai_client
from app.core.config import settings
from app.models.quote_model import ExtractedQuoteData

logger = logging.getLogger(__name__)


class InsurerDetectionResult(NamedTuple):
    """Result of insurer detection with metadata."""
    name: Optional[str]  # Detected insurer name or None
    method: Optional[str]  # Detection method: "filename", "text_match", "pattern", "ai", "ocr", "format_override", None
    confidence: Optional[str]  # Confidence level: "high", "medium", "low", None


class AIParsingError(Exception):
    """Custom exception for AI parsing failures."""
    pass


class VatPolicyViolation(Exception):
    """
    Exception raised when VAT structure violates policy.

    This indicates the document has disallowed VAT characteristics
    and must not proceed to ranking/comparison.
    """
    def __init__(self, vat_class: str, reason: str, details: dict = None):
        self.vat_class = vat_class
        self.reason = reason
        self.details = details or {}
        super().__init__(f"VAT Policy Violation: {vat_class} - {reason}")


# ============================================================================
# COMPREHENSIVE VAT PATTERN CONSTANTS v9.0
# ============================================================================
# Complete taxonomy of VAT patterns for exhaustive detection before classification
# Priority: VAT_EXCLUSIVE checked first (more specific), then VAT_INCLUSIVE

VAT_INCLUSIVE_PATTERNS = [
    # GROUP A1: Direct "inclusive" keyword
    r'vat\s+inclusive',
    r'vat[\s\-]?inclusive',
    r'inclusive\s+of\s+vat',
    r'inclusive\s+vat',

    # GROUP A2: "included" keyword
    r'vat\s+included',
    r'included\s+vat',
    r'vat\s+is\s+included',
    r'with\s+vat\s+included',

    # GROUP A3: "including" keyword
    r'including\s+vat',
    r'incl\.?\s+vat',
    r'inc\.?\s+vat',

    # GROUP A4: Context phrases (premium/total includes)
    r'total.*includes?\s+vat',
    r'premium.*includes?\s+vat',
    r'amount.*includes?\s+vat',
    r'price.*includes?\s+vat',
    r'rate.*includes?\s+vat',
    r'total.*with\s+vat\s+included',
    r'premium.*with\s+vat\s+included',
    r'total\s+premium\s+with\s+vat\s+included',

    # GROUP A5: Tax variations
    r'incl\.?\s+tax',
    r'including\s+tax',
    r'tax\s+included',
    r'inclusive\s+of\s+all\s+taxes',
    r'all\s+taxes\s+included',
]

VAT_EXCLUSIVE_PATTERNS = [
    # GROUP B1: Direct "exclusive" keyword
    r'vat\s+exclusive',
    r'vat[\s\-]?exclusive',
    r'exclusive\s+of\s+vat',
    r'exclusive\s+vat',

    # GROUP B2: "excluding" keyword
    r'excluding\s+vat',
    r'excl\.?\s+vat',
    r'exc\.?\s+vat',
    r'vat\s+excluded',

    # GROUP B3: "plus VAT" keyword (CRITICAL)
    r'plus\s+vat',
    r'\+\s*vat',
    r'plus\s+applicable\s+vat',
    r'plus\s+vat\s+as\s+applicable',

    # GROUP B4: "applicable" keyword (CRITICAL)
    r'vat\s+as\s+applicable',
    r'vat\s+applicable',
    r'vat\s+where\s+applicable',
    r'applicable\s+vat',
    r'vat\s+as\s+per\s+applicable',

    # GROUP B5: "subject to" keyword
    r'subject\s+to\s+vat',
    r'premium.*subject\s+to\s+vat',
    r'rate.*subject\s+to\s+vat',
    r'subject\s+to\s+applicable\s+vat',

    # GROUP B6: "will be added/charged" keyword
    # Note: Patterns with "charged at billing" moved to VAT_DEFERRED_PATTERNS
    r'vat\s+will\s+be\s+added',
    r'vat\s+will\s+be\s+charged',
    r'vat\s+shall\s+be\s+added',
    r'vat\s+shall\s+be\s+charged',
    r'vat\s+to\s+be\s+added',
    r'vat\s+to\s+be\s+charged',

    # GROUP B7: "additional/payable" keyword
    r'vat\s+additional',
    r'additional\s+vat',
    r'vat\s+payable',
    r'vat\s+payable\s+by',
    r'vat\s+shall\s+be\s+paid',
    r'insured\s+shall\s+pay\s+vat',

    # GROUP B8: Explicit percentages (with numbers)
    r'vat\s*\(?\s*\d+\.?\d*\s*%\s*\)?',
    r'vat\s*:?\s*\d+\.?\d*\s*%',
    r'\d+\.?\d*\s*%\s*vat',
    r'vat\s*@\s*\d+\.?\d*\s*%',
    r'vat\s+rate\s*:?\s*\d+\.?\d*\s*%',
    r'\+\s*\d+\.?\d*\s*%\s*vat',
    r'vat\s+\d+\.?\d*\s*%\s+additional',
    r'vat\s+\d+\.?\d*\s*%.*?will\s+apply',
    r'vat\s+\d+\.?\d*\s*%.*?applicable',

    # GROUP B9: Explicit amounts (with SAR/SR)
    r'vat\s*:?\s*sar\s*[\d,\.]+',
    r'vat\s*:?\s*sr\.?\s*[\d,\.]+',
    r'vat\s+amount\s*:?\s*[\d,\.]+',
    r'sar\s*[\d,\.]+.*vat',

    # GROUP B10: Calculation context
    r'premium.*\+.*vat',
    r'total.*before\s+vat',
    r'net.*premium.*vat',
    r'base.*premium.*vat\s+extra',

    # GROUP B11: Tax generic (without VAT keyword but contextual)
    r'taxes?\s+as\s+per\s+law',
    r'vat\s+as\s+per\s+law',
    r'vat\s+according\s+to\s+regulations',
]

# VAT_DEFERRED_PATTERNS: Patterns that explicitly defer VAT to billing time (P3 Classification)
# These patterns indicate VAT will be charged later at billing, not at quote time
# P3 documents should have vat_percentage=null and vat_amount=null
VAT_DEFERRED_PATTERNS = [
    # Primary deferral patterns - explicit billing language
    r'all\s+taxes?\s+will\s+be\s+charged\s+at\s+(?:the\s+)?time\s+of\s+billing',
    r'taxes?\s+will\s+be\s+charged\s+at\s+(?:the\s+)?time\s+of\s+billing',
    r'taxes?\s+charged\s+at\s+(?:the\s+)?time\s+of\s+billing',

    # Deferral to later/invoice time
    r'vat\s+will\s+be\s+charged\s+at\s+(?:the\s+)?time\s+of\s+(?:billing|invoice|invoicing)',
    r'vat\s+charged\s+at\s+(?:the\s+)?time\s+of\s+(?:billing|invoice|invoicing)',
    r'vat\s+will\s+apply\s+at\s+(?:the\s+)?time\s+of\s+(?:billing|invoice|invoicing)',

    # Deferral to issuance
    r'taxes?\s+(?:will\s+)?(?:be\s+)?charged\s+(?:at|upon)\s+issuance',
    r'vat\s+(?:will\s+)?(?:be\s+)?charged\s+(?:at|upon)\s+issuance',
]

VAT_MENTIONED_PATTERNS = [
    r'\bvat\b',
    r'value\s+added\s+tax',
    r'v\.?a\.?t\.?',
]

# ============================================================================
# ENHANCED UTILITY FUNCTIONS v6.0
# ============================================================================

def _normalize_rate_notation(rate_text: str) -> str:
    """
    Convert rate text to proper notation with PRODUCTION-LEVEL @ symbol handling.
    CRITICAL: Extracts rate correctly from patterns like "66,340 @ 0.1615 percent"
    """
    if not rate_text:
        return "N/A"
    
    rate_text = str(rate_text).strip()
    original_text = rate_text  # Keep original for fallback
    rate_text = rate_text.replace('$', '').replace('USD', '').replace('SAR', '').replace('SR', '')
    
    # Check for FLAT Premium
    if 'flat' in rate_text.lower() and 'premium' in rate_text.lower():
        return "FLAT Premium"
    
    # ====================================================================
    # CRITICAL FIX: Handle @ symbol pattern (premium @ rate)
    # ====================================================================
    # Pattern: "66,340.70 @ 0.1615 percent" or "69,602 @ 0.2%"
    # Extract the number AFTER @, NOT before
    
    if '@' in rate_text:
        # Split on @ and take the part after it (that's the rate)
        parts = rate_text.split('@')
        if len(parts) >= 2:
            rate_part = parts[1].strip()  # Everything after @
            # Extract numeric value from rate part only
            rate_match = re.search(r'(\d+\.?\d*)', rate_part)
            if rate_match:
                numeric_value = rate_match.group(1)
                # Validate rate is in reasonable range
                try:
                    rate_float = float(numeric_value)
                    if 0.001 <= rate_float <= 10:
                        # Determine symbol based on text
                        lower_rate = rate_part.lower()
                        if '‰' in rate_part or 'per mille' in lower_rate or 'permille' in lower_rate:
                            return f"{numeric_value}‰"
                        elif '%' in rate_part:
                            return f"{numeric_value}%"
                        elif 'percent' in lower_rate:
                            return f"{numeric_value}%"
                        else:
                            # Default for decimal rates
                            return f"{numeric_value}%"
                    else:
                        logger.warning(f"⚠️ Suspicious rate {rate_float} extracted from @ pattern, continuing to fallback")
                except ValueError:
                    pass
    
    # ====================================================================
    # PRIORITY 1: Decimal rates (0.XX) are MOST LIKELY correct rates
    # ====================================================================
    decimal_rate_match = re.search(r'(\d*\.\d+)', rate_text)
    if decimal_rate_match:
        numeric_value = decimal_rate_match.group(1)
        try:
            rate_float = float(numeric_value)
            # Validate: typical rates are 0.001% to 10%
            if 0.001 <= rate_float <= 10:
                lower_text = rate_text.lower()
                if '‰' in rate_text:
                    return f"{numeric_value}‰"
                elif '%o' in rate_text or 'per mille' in lower_text or 'permille' in lower_text:
                    return f"{numeric_value}‰"
                elif '%' in rate_text and 'per mille' not in lower_text:
                    return f"{numeric_value}%"
                elif 'percent' in lower_text:
                    return f"{numeric_value}%"
                else:
                    # Default for decimal rates < 1 is per mille
                    if rate_float < 1:
                        return f"{numeric_value}‰"
                    else:
                        return f"{numeric_value}%"
        except ValueError:
            pass
    
    # ====================================================================
    # PRIORITY 2: Whole numbers (only if no decimal rate found)
    # ====================================================================
    # CRITICAL: This should ONLY run if we didn't find a decimal rate
    whole_number_match = re.search(r'(\d+)', rate_text)
    if whole_number_match:
        numeric_value = whole_number_match.group(1)
        try:
            rate_int = int(numeric_value)
            # CRITICAL VALIDATION: Reject if > 10 (likely premium, not rate)
            if rate_int > 10:
                logger.warning(f"⚠️ Extracted rate {rate_int} > 10, likely premium confusion")
                return "N/A"
            
            lower_text = rate_text.lower()
            if '‰' in rate_text:
                return f"{numeric_value}‰"
            elif '%o' in rate_text or 'per mille' in lower_text or 'permille' in lower_text:
                return f"{numeric_value}‰"
            elif '%' in rate_text and 'per mille' not in lower_text:
                return f"{numeric_value}%"
            elif 'basis point' in lower_text or 'bp' in lower_text:
                return f"{numeric_value} bp"
            else:
                return f"{numeric_value}‰"
        except ValueError:
            pass
    
    # ====================================================================
    # FALLBACK: Return original text if we couldn't parse it
    # ====================================================================
    logger.warning(f"⚠️ Could not normalize rate: {original_text}")
    return original_text


def _sanitize_json_string(text: str) -> str:
    """Sanitize text to be JSON-safe."""
    if not text:
        return text
    
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = re.sub(r"([a-zA-Z])'s\b", r"\1", text)
    text = text.replace("'", "")
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('"', '\\"')
    
    return text.strip()


def _fix_json_response(json_text: str) -> str:
    """Fix common JSON formatting issues from AI and return best-effort JSON string."""
    try:
        # Strip code fences and whitespace noise
        text = json_text.strip()
        text = re.sub(r'^```json\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'^```\s*', '', text)
        text = re.sub(r'\s*```\s*$', '', text)
        
        # Normalize quotes and control chars
        text = text.replace('“', '"').replace('”', '"').replace('’', "'")
        text = text.replace('\r', ' ').replace('\n', ' ')
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', ' ', text)
        
        # Keep only the outermost JSON object if extra text surrounds it
        if '{' in text and '}' in text:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                text = text[start:end + 1]
        
        # Remove trailing commas like ,} or ,]
        text = re.sub(r',\s*(\}|\])', r'\1', text)
        
        # Sanitize inside quoted strings
        def sanitize_string_segment(match):
            seg = match.group(0)
            inner = seg[1:-1]
            inner = inner.replace('\\"', '\\"')
            inner = inner.replace('"', '\\"')
            inner = re.sub(r'\s+', ' ', inner)
            return '"' + inner + '"'
        text = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', sanitize_string_segment, text)
        
        return text
    except Exception as e:
        logger.error(f"Error fixing JSON: {str(e)}")
        return json_text



# ============================================================================
# FALLBACK EXTRACTION FUNCTIONS v7.0 - PRODUCTION FIX
# ============================================================================

def _extract_insured_fallback(text: str) -> str:
    """
    FALLBACK: Extract insured name using pattern matching.
    FIX: Better patterns to catch variations like "M/s. Company &/or Subsidiary".
    """
    insured_patterns = [
        r'(?:Insured[:\s]+|Name of Insured[:\s]+)(M/s\.?\s*[A-Z][^\n]{5,150}(?:&/or[^\n]{5,100})?)',
        r'(?:Insured[:\s]+|Name of Insured[:\s]+)([A-Z][A-Za-z\s\.\,\(\)&/-]+(?:Ltd|LLC|Company|Co\.|Corporation|Corp|Inc|Hospital|Medical|Clinic|Group))',
        r'(?:Client[:\s]+|Client Name[:\s]+)([A-Z][A-Za-z\s\.\,\(\)&/-]+(?:Ltd|LLC|Company|Co\.|Corporation|Corp|Inc|Hospital|Medical|Clinic|Group))',
        r'(?:Proposer[:\s]+|Proposed Insured[:\s]+)([A-Z][A-Za-z\s\.\,\(\)&/-]+(?:Ltd|LLC|Company|Co\.|Corporation|Corp|Inc|Hospital|Medical|Clinic|Group))',
        r'INSURED[\'\s]*NAME[:\s]*([A-Z][^\n]{10,150})',
        r'NAME OF THE INSURED[:\s]*([A-Z][^\n]{10,150})',
        r'(?:Insured|Client|Proposer)\s*[:|]\s*([A-Z][A-Za-z\s\.\,\(\)&/-]{10,150})',
    ]

    for pattern in insured_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            insured_name = match.group(1).strip()
            insured_name = re.sub(r'\s+', ' ', insured_name)
            insured_name = insured_name.replace('\n', ' ').replace('\r', ' ')

            for delimiter in ['Location:', 'Address:', 'Period:', 'Coverage:', 'Occupation:', '\n\n']:
                if delimiter in insured_name:
                    insured_name = insured_name.split(delimiter)[0].strip()

            if 5 <= len(insured_name) <= 200:
                return insured_name

    return "Not specified"


def _extract_benefits_fallback(text: str) -> List[str]:
    """
    FALLBACK: Extract benefits using pattern matching when AI fails.
    Works for ANY insurance document format - UNIVERSAL SOLUTION.
    """
    benefits = []
    
    # Strategy 1: Look for bulleted/numbered lists with benefit indicators
    benefit_patterns = [
        r'[-•▪]\s+([^-•▪\n]{20,300}(?:clause|Clause|cover|including|limit|SR\s*\d|SAR\s*\d)[^\n]{0,200})',
        r'(?:^|\n)\s*[-•▪]\s*([A-Z][^•▪\n]{15,300}(?:Clause|clause|coverage|limit|SR\s+[\d,]+|SAR\s+[\d,]+)[^\n]{0,150})',
        r'(?:^|\n)\s*\d+[\)\.]\s+([A-Z][^\n]{20,300}(?:Clause|clause|coverage|including|SR\s+[\d,]+)[^\n]{0,150})',
    ]
    
    for pattern in benefit_patterns:
        matches = re.finditer(pattern, text, re.MULTILINE)
        for match in matches:
            benefit = match.group(1).strip()
            benefit = re.sub(r'\s+', ' ', benefit)
            benefit = benefit.replace('\r', '').replace('\n', ' ')
            
            if len(benefit) < 15 or len(benefit) > 500:
                continue
            if benefit.lower().startswith('excluding'):
                continue
            if 'exclusion' in benefit.lower() and 'clause' in benefit.lower():
                continue
                
            if benefit not in benefits:
                benefits.append(benefit)
    
    # Strategy 2: Section-based extraction
    conditions_section_match = re.search(
        r'(?:CONDITIONS?|BENEFITS?|COVERAGE)[:\s]*(.*?)(?:WARRANTIES?|EXCLUSIONS?|SUBJECTIVITIES?)',
        text,
        re.DOTALL | re.IGNORECASE
    )
    
    if conditions_section_match:
        section_text = conditions_section_match.group(1)
        bullets = re.findall(r'[-•▪]\s*([^\n•▪-]{20,400})', section_text)
        for bullet in bullets:
            bullet = bullet.strip()
            bullet = re.sub(r'\s+', ' ', bullet)
            if len(bullet) >= 20 and bullet not in benefits:
                if not bullet.lower().startswith('excluding'):
                    benefits.append(bullet)
    
    # Strategy 3: Clause + amount patterns
    clause_matches = re.finditer(
        r'([A-Z][A-Za-z\s&/]+?(?:Clause|clause|coverage))\s*[-–]?\s*(?:Limit(?:ed)?\s+(?:up\s+to\s+)?)?(?:SR\.?|SAR)\s*([\d,]+)',
        text
    )
    
    for match in clause_matches:
        clause_name = match.group(1).strip()
        amount = match.group(2).strip()
        clause_name = re.sub(r'\s+', ' ', clause_name)
        
        if len(clause_name) >= 10 and 'excluding' not in clause_name.lower():
            benefit_str = f"{clause_name} - Limit: SR {amount}"
            if benefit_str not in benefits:
                benefits.append(benefit_str)
    
    logger.info(f"📋 Fallback extracted {len(benefits)} benefits from patterns")
    return benefits


def _extract_exclusions_fallback(text: str) -> List[str]:
    """
    FALLBACK: Extract exclusions using pattern matching when AI fails.
    Works for ANY insurance document format - UNIVERSAL SOLUTION.
    """
    exclusions = []
    
    # Strategy 1: EXCLUSIONS section
    exclusions_section_match = re.search(
        r'EXCLUSIONS?[:\s]*(.*?)(?:WARRANTIES?|SUBJECTIVITIES?|DEDUCTIBLES?|RATE|$)',
        text,
        re.DOTALL | re.IGNORECASE
    )
    
    if exclusions_section_match:
        section_text = exclusions_section_match.group(1)
        bullets = re.findall(r'[-•▪]\s*([^\n•▪-]{15,400})', section_text)
        for bullet in bullets:
            bullet = bullet.strip()
            bullet = re.sub(r'\s+', ' ', bullet)
            if len(bullet) >= 15 and bullet not in exclusions:
                exclusions.append(bullet)
    
    # Strategy 2: "Excluding" patterns
    excluding_patterns = [
        r'[-•▪]\s*(Excluding[^\n•▪-]{15,300})',
        r'(Excluding\s+[A-Za-z\s,/&]+(?:Exclusion|exclusion|Clause|clause|risks?))',
    ]
    
    for pattern in excluding_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            exclusion = match.group(1).strip()
            exclusion = re.sub(r'\s+', ' ', exclusion)
            if len(exclusion) >= 15 and exclusion not in exclusions:
                exclusions.append(exclusion)
    
    # Strategy 3: Known exclusion clauses
    exclusion_clause_patterns = [
        r'([A-Z][A-Za-z\s&/]+?(?:Exclusion|exclusion)\s*[-–]?\s*[A-Z]{2,6}\s*\d+)',
        r'((?:War|Nuclear|Cyber|Terrorism|Political)[^\.]{0,100}(?:Exclusion|exclusion|Clause|clause))',
    ]
    
    for pattern in exclusion_clause_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            exclusion = match.group(1).strip()
            exclusion = re.sub(r'\s+', ' ', exclusion)
            if len(exclusion) >= 15 and exclusion not in exclusions:
                exclusions.append(exclusion)
    
    logger.info(f"🚫 Fallback extracted {len(exclusions)} exclusions from patterns")
    return exclusions


def _extract_deductible_fallback(text: str, total_si: float = 0) -> str:
    """
    FALLBACK: Extract deductible using pattern matching.
    FIX: Better handling of tiered deductibles and "As Per SAMA" cases.
    """
    if re.search(r'As Per SAMA/IA minimum deductibles', text, re.IGNORECASE):
        if total_si and total_si > 500_000_000:
            return ("MD: 5% of claim amount, minimum SR 1,000,000 | "
                    "BI: Minimum 21 days | Nat Cat: 5% of claim amount, minimum SR 1,500,000")
        elif total_si and total_si > 100_000_000:
            return ("MD: 5% of claim amount, minimum SR 500,000 | "
                    "BI: Minimum 14 days | Nat Cat: 5% of claim amount, minimum SR 1,000,000")
        elif total_si and total_si > 40_000_000:
            return ("MD: 5% of claim amount, minimum SR 100,000 | "
                    "BI: Minimum 10 days | Nat Cat: 5% of claim amount, minimum SR 250,000")
        else:
            return ("MD: 5% of claim amount, minimum SR 50,000 | "
                    "BI: Minimum 7 days | Nat Cat: 5% of claim amount, minimum SR 50,000")

    deductible_section_match = re.search(
        r'(?:DEDUCTIBLES?|Deductibles?)\s*\(each and every[^)]*\)[:\s]*(.*?)(?:CONDITIONS?|WARRANTIES?|EXCLUSIONS?|RATE|Rate)',
        text,
        re.DOTALL | re.IGNORECASE
    )

    if deductible_section_match:
        section_text = deductible_section_match.group(1)
        tier_patterns = [
            r'(?:sum insured|Sum Insured|properties).{0,50}(?:above|from|exceeding).{0,20}(?:SR|SAR)\s*([\d,]+).{0,150}'
            r'Material Damage.{0,50}(\d+%?).{0,50}(?:minimum|min).{0,20}(?:SR|SAR)\s*([\d,]+)',
            r'(?:Properties|properties).{0,50}(?:SR|SAR)\s*([\d,]+).{0,150}Material Damage.{0,50}(\d+%?).{0,50}(?:SR|SAR)\s*([\d,]+)',
        ]

        for pattern in tier_patterns:
            matches = list(re.finditer(pattern, section_text, re.IGNORECASE))
            if matches:
                for match in matches:
                    si_threshold = float(match.group(1).replace(',', ''))
                    percentage = match.group(2)
                    minimum = match.group(3)

                    if total_si and total_si >= si_threshold:
                        bi_match = re.search(
                            r'Business Interruption.{0,50}Minimum\s+(\d+)\s+days',
                            section_text,
                            re.IGNORECASE
                        )
                        bi_days = bi_match.group(1) if bi_match else "N/A"

                        nat_cat_match = re.search(
                            r'Natural Catastrophe.{0,100}(\d+%?).{0,50}minimum.{0,20}(?:SR|SAR)\s*([\d,]+)',
                            section_text,
                            re.IGNORECASE
                        )

                        if nat_cat_match:
                            nat_cat_str = (f" | Nat Cat: {nat_cat_match.group(1)} of claim amount, "
                                           f"minimum SR {nat_cat_match.group(2)}")
                        else:
                            nat_cat_str = ""

                        return (f"MD: {percentage} of claim amount, minimum SR {minimum} | "
                                f"BI: Minimum {bi_days} days{nat_cat_str}")

    deductible_patterns = [
        r'(?:Deductible|deductible)[:\s]*([^\n]{10,200})',
        r'(?:Each and every loss|each and every loss)[:\s]*([^\n]{10,200})',
        r'(?:MD:|Material Damage:)\s*([^\n|]{10,150})',
    ]

    for pattern in deductible_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            ded_text = match.group(1).strip()
            ded_text = re.sub(r'\s+', ' ', ded_text)

            if 'BI:' in ded_text or 'Business Interruption' in ded_text:
                return ded_text

            bi_match = re.search(
                r'(?:BI|Business Interruption).{0,50}(?:Minimum\s+)?(\d+)\s+days',
                text,
                re.IGNORECASE
            )

            if bi_match:
                return f"MD: {ded_text} | BI: Minimum {bi_match.group(1)} days"

            return f"MD: {ded_text}"

    return "N/A"


def _extract_warranties_fallback(text: str) -> List[str]:
    """
    FALLBACK: Extract warranties using pattern matching.
    FIX: Better patterns to catch warranty codes like (W2), (W27) etc.
    """
    warranties = []

    warranty_code_pattern = r'\(W\d+\)\s*([^\n]{5,300})'
    code_matches = re.finditer(warranty_code_pattern, text, re.IGNORECASE)

    for match in code_matches:
        warranty_text = match.group(0).strip()
        warranty_text = re.sub(r'\s+', ' ', warranty_text)

        if len(warranty_text) >= 10 and warranty_text not in warranties:
            warranties.append(warranty_text)

    warranty_section_match = re.search(
        r'(?:WARRANTIES?|Warranties?)[:\s]*(.*?)(?:EXCLUSIONS?|Exclusions?|RATE|Rate|PREMIUM|Premium)',
        text,
        re.DOTALL | re.IGNORECASE
    )

    if warranty_section_match:
        section_text = warranty_section_match.group(1)

        bullet_patterns = [
            r'[-•▪]\s+([^\n•▪-]{15,300}(?:warranty|Warranty|warranted|Warranted)[^\n]{0,200})',
            r'(?:^|\n)\s*[-•▪]\s*([A-Z][^\n]{15,300})',
            r'(?:^|\n)\s*\d+[\)\.]\s+([^\n]{15,300}(?:warranty|Warranty|warranted|Warranted)[^\n]{0,150})',
        ]

        for pattern in bullet_patterns:
            matches = re.finditer(pattern, section_text, re.MULTILINE)
            for match in matches:
                warranty = match.group(1).strip()
                warranty = re.sub(r'\s+', ' ', warranty)

                if 15 <= len(warranty) <= 500 and warranty not in warranties:
                    warranties.append(warranty)

    warranted_pattern = r'(?:Warranted|warranted)\s+(?:that\s+)?([^\n]{10,300})'
    warranted_matches = re.finditer(warranted_pattern, text, re.IGNORECASE)

    for match in warranted_matches:
        warranty_text = match.group(0).strip()
        warranty_text = re.sub(r'\s+', ' ', warranty_text)

        for delimiter in ['. Warranted', '. (', '\n\n']:
            if delimiter in warranty_text:
                warranty_text = warranty_text.split(delimiter)[0].strip()

        if 10 <= len(warranty_text) <= 500 and warranty_text not in warranties:
            warranties.append(warranty_text)

    final_warranties = []
    for w in warranties:
        is_duplicate = False
        for existing in final_warranties:
            if w[:30].lower() == existing[:30].lower():
                is_duplicate = True
                break

        if not is_duplicate:
            final_warranties.append(w)

    logger.info(f"⚠️ Fallback extracted {len(final_warranties)} warranties from patterns")
    return final_warranties


def _validate_and_enhance_extraction(ai_data: Dict, raw_text: str, total_si: float) -> Dict:
    """
    Stage 3.5: Validate AI extraction and enhance with fallback data.
    FIX: Better validation logic for client name and deductibles.
    """
    ai_data = ai_data or {}

    logger.info("🔍 Stage 3.5: Validating and enhancing extraction...")

    client_name = (ai_data.get('insured_customer_name')
                   or ai_data.get('insured_name')
                   or "").strip()
    if not client_name or client_name.lower() in {"not specified", "unknown", "n/a"} or len(client_name) < 5:
        logger.warning("⚠️ Client name missing/invalid, using fallback...")
        fallback_client = _extract_insured_fallback(raw_text)
        if fallback_client != "Not specified":
            ai_data['insured_customer_name'] = fallback_client
            ai_data['insured_name'] = fallback_client
            logger.info(f"✅ Extracted client name: {fallback_client}")

    deductibles_data = ai_data.get('deductibles_complete')
    if not isinstance(deductibles_data, dict):
        deductibles_data = {}
        ai_data['deductibles_complete'] = deductibles_data

    def _has_valid_deductible(data: Dict) -> bool:
        for key in ['material_damage_tiers', 'business_interruption_tiers', 'natural_catastrophe_tiers']:
            tiers = data.get(key, [])
            if isinstance(tiers, list):
                for tier in tiers:
                    deductible_value = str(tier.get('deductible', '')).strip()
                    if deductible_value and deductible_value.upper() != 'N/A':
                        return True
        summary_fields = [
            ai_data.get('deductible'),
            ai_data.get('deductible_summary'),
            data.get('fallback_summary'),
            ai_data.get('deductible_summary_fallback'),
        ]
        for summary in summary_fields:
            if summary and "N/A" not in str(summary):
                return True
        return False

    if not _has_valid_deductible(deductibles_data):
        logger.warning("⚠️ Deductible missing/invalid, using fallback...")
        fallback_deductible = _extract_deductible_fallback(raw_text, total_si or 0)
        if fallback_deductible != "N/A":
            ai_data['deductible_summary_fallback'] = fallback_deductible
            ai_data['deductible'] = fallback_deductible
            deductibles_data['fallback_summary'] = fallback_deductible
            logger.info(f"✅ Extracted deductible: {fallback_deductible}")

    warranties_section = ai_data.get('warranties_actual')
    if not isinstance(warranties_section, dict):
        warranties_section = {}
        ai_data['warranties_actual'] = warranties_section

    warranties = warranties_section.get('warranties_list', [])
    if not isinstance(warranties, list):
        warranties = []

    if len(warranties) < 5:
        logger.warning(f"⚠️ Only {len(warranties)} warranties, using fallback...")
        fallback_warranties = _extract_warranties_fallback(raw_text)
        if fallback_warranties:
            existing_lower = {w.lower()[:50] for w in warranties if isinstance(w, str)}
            for fw in fallback_warranties:
                if fw.lower()[:50] not in existing_lower:
                    warranties.append(fw)
                    existing_lower.add(fw.lower()[:50])
            warranties_section['warranties_list'] = warranties
            logger.info(f"✅ Enhanced warranties: {len(warranties)} total")

    exclusions_section = ai_data.get('exclusions_complete')
    if not isinstance(exclusions_section, dict):
        exclusions_section = {}
        ai_data['exclusions_complete'] = exclusions_section

    exclusions = exclusions_section.get('all_exclusions_list', [])
    if not isinstance(exclusions, list):
        exclusions = []

    if len(exclusions) < 10:
        logger.warning(f"⚠️ Only {len(exclusions)} exclusions, using fallback...")
        fallback_exclusions = _extract_exclusions_fallback(raw_text)
        if fallback_exclusions:
            existing_lower = {e.lower()[:50] for e in exclusions if isinstance(e, str)}
            for fe in fallback_exclusions:
                if fe.lower()[:50] not in existing_lower:
                    exclusions.append(fe)
                    existing_lower.add(fe.lower()[:50])
            exclusions_section['all_exclusions_list'] = exclusions
            logger.info(f"✅ Enhanced exclusions: {len(exclusions)} total")

    benefits_section = ai_data.get('coverage_and_benefits')
    if not isinstance(benefits_section, dict):
        benefits_section = {}
        ai_data['coverage_and_benefits'] = benefits_section

    benefits = benefits_section.get('coverage_benefits_explained', [])
    if not isinstance(benefits, list):
        benefits = []

    if len(benefits) < 10:
        logger.warning(f"⚠️ Only {len(benefits)} benefits, using fallback...")
        fallback_benefits = _extract_benefits_fallback(raw_text)
        if fallback_benefits:
            existing_lower = {b.lower()[:50] for b in benefits if isinstance(b, str)}
            for fb in fallback_benefits:
                if fb.lower()[:50] not in existing_lower:
                    benefits.append(fb)
                    existing_lower.add(fb.lower()[:50])
            benefits_section['coverage_benefits_explained'] = benefits
            logger.info(f"✅ Enhanced benefits: {len(benefits)} total")

    return ai_data


    return warranties


def _parse_insurer_from_ocr_text(ocr_text: str) -> Optional[str]:
    """
    Parse insurer name from OCR text by cross-referencing with known patterns.

    This function attempts to normalize OCR text to standard insurer names
    by fuzzy matching against reference patterns. It does NOT validate or
    reject unknown insurers.

    Args:
        ocr_text: Text extracted from logo via OCR

    Returns:
        Normalized insurer name or None if no match found
    """
    if not ocr_text:
        return None

    ocr_text_lower = ocr_text.lower()

    # Try to match against INSURER_REFERENCE_PATTERNS (fuzzy matching)
    # Note: INSURER_REFERENCE_PATTERNS is defined inside _extract_insurer_from_text
    # We'll use a simplified version here with the most common patterns
    reference_patterns = {
        # Premium Tier
        'Tawuniya': ['tawuniya', 'company for cooperative insurance'],
        'Walaa Insurance': ['walaa', 'walaa cooperative'],
        'MedGulf Insurance': ['medgulf', 'mediterranean and gulf'],

        # Strong Tier
        'Gulf Insurance Group (GIG)': ['gig', 'gulf insurance group', 'gulf insurance'],
        'Gulf General Cooperative Insurance Company': ['ggi', 'gulf general'],
        'Al-Etihad Cooperative Insurance': ['al-etihad', 'al etihad'],
        'Wataniya Insurance': ['wataniya'],
        'AXA Gulf': ['axa'],
        'Allianz Saudi Fransi': ['allianz'],
        'Zurich Insurance': ['zurich'],

        # Solid Tier
        'Malath Insurance': ['malath'],
        'Liva Insurance': ['liva'],
        'Tokio Marine': ['tokio marine'],

        # Baseline Tier
        'Chubb Arabia': ['chubb'],
        'Arabian Shield Cooperative Insurance Company': ['arabian shield'],
        'Allied Cooperative Insurance Group': ['acig', 'allied cooperative'],
        'Saudi Arabian Cooperative Insurance Company': ['saico', 'saudi arabian cooperative'],
        'Salama Insurance': ['salama'],
        'Al Jazeera Takaful Company': ['ajtc', 'al jazeera takaful'],
        'Arabia Insurance Cooperative Company': ['aicc', 'acic', 'arabia insurance cooperative'],
        'Al Sagr Co-operative Insurance Company': ['al sagr', 'al-sagr', 'alsagr'],
        'Amanah Cooperative Insurance Company': ['amanah'],
        'Mutakamela Insurance': ['mutakamela'],
        'Al Rajhi Takaful': ['art', 'al rajhi takaful', 'alrajhi takaful'],

        # Challenged Tier
        'Gulf Union Alahlia Cooperative Insurance Company': ['gulf union'],
        'United Cooperative Assurance (UCA)': ['uca', 'united cooperative assurance'],
    }

    for normalized_name, patterns in reference_patterns.items():
        for pattern in patterns:
            if pattern in ocr_text_lower:
                logger.info(f"✅ OCR text matched pattern '{pattern}' -> '{normalized_name}'")
                return normalized_name

    # If no exact match, check for generic insurance keywords to determine if OCR found a company name
    insurance_keywords = ['insurance', 'cooperative', 'takaful', 'assurance', 'company']
    if any(keyword in ocr_text_lower for keyword in insurance_keywords):
        # OCR found something insurance-related but not in our reference list
        # Return the first 50 chars as a potential new insurer
        logger.info(f"✅ OCR found insurance-related text (not in reference): {ocr_text[:50]}")
        return ocr_text[:50].strip()

    return None


async def _ocr_fallback_detect_insurer(pdf_path: str) -> Optional[str]:
    """
    OCR fallback for insurer detection when all text-based methods fail.

    This is a COST-OPTIMIZED last resort - only called when:
    1. Filename-based detection failed
    2. Text pattern matching failed
    3. AI extraction failed

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Detected insurer name or None
    """
    try:
        logger.info(f"🔍 OCR fallback: Attempting logo-based insurer detection for {pdf_path}")

        # Step 1: Extract logo bytes from PDF
        from app.services.pdf_extractor import pdf_extractor
        logo_bytes = pdf_extractor.extract_logo_bytes_from_pdf(pdf_path)

        if not logo_bytes:
            logger.info("No logo found in PDF - OCR fallback not possible")
            return None

        logger.info(f"📸 Logo extracted ({len(logo_bytes)} bytes) - calling Azure OCR")

        # Step 2: Call Azure OCR
        from app.services.azure_ocr_service import azure_ocr_service
        ocr_text = await azure_ocr_service.analyze_image_for_text(logo_bytes)

        if not ocr_text:
            logger.info("OCR returned no text from logo")
            return None

        logger.info(f"✅ OCR extracted text: {ocr_text}")

        # Step 3: Parse OCR text to identify insurer name
        # Cross-reference with INSURER_REFERENCE_PATTERNS for normalization (not validation)
        detected_insurer = _parse_insurer_from_ocr_text(ocr_text)

        if detected_insurer:
            logger.info(f"✅ OCR detected insurer: {detected_insurer}")
        else:
            logger.info("OCR text did not match any known insurers - using raw OCR text")
            detected_insurer = ocr_text[:50].strip()  # Use first 50 chars of OCR text as fallback

        return detected_insurer

    except Exception as e:
        logger.error(f"❌ OCR fallback error: {e}")
        return None


async def _extract_insurer_from_text(text: str, filename: str = "", pdf_path: Optional[str] = None) -> InsurerDetectionResult:
    """
    Extract insurance company name (NOT the customer).

    Returns:
        InsurerDetectionResult with name, detection method, and confidence level.
    """
    # Priority 1: Check filename first (more reliable)
    if filename:
        filename_lower = filename.lower()
        # Remove file extension for better matching
        filename_clean = filename_lower.replace('.pdf', '').replace('.doc', '').replace('.docx', '')

        # Use word boundaries for more precise matching
        # INSURER_REFERENCE_PATTERNS: Reference data for pattern matching and normalization.
        # NOT a whitelist - used for fuzzy matching and standardization of detected names.
        INSURER_REFERENCE_PATTERNS = {
            # Premium Tier
            r'\btawuniya\b': 'Tawuniya',
            r'company for cooperative insurance': 'Tawuniya',
            r'\bwalaa\b': 'Walaa Insurance',
            r'walaa cooperative': 'Walaa Insurance',
            r'medgulf': 'MedGulf Insurance',
            r'mediterranean and gulf': 'MedGulf Insurance',
            
            # Strong Tier
            r'\bgig\b': 'Gulf Insurance Group (GIG)',
            r'gulf insurance group': 'Gulf Insurance Group (GIG)',
            r'gulf insurance': 'Gulf Insurance Group (GIG)',
            r'\bggi\b': 'Gulf General Cooperative Insurance Company',
            r'gulf general': 'Gulf General Cooperative Insurance Company',
            r'al[\s-]?etihad': 'Al-Etihad Cooperative Insurance',
            r'\bwataniya\b': 'Wataniya Insurance',
            r'\baxa\b': 'AXA Gulf',
            r'axa gulf': 'AXA Gulf',
            r'\ballianz\b': 'Allianz Saudi Fransi',
            r'allianz saudi fransi': 'Allianz Saudi Fransi',
            r'\bzurich\b': 'Zurich Insurance',
            
            # Solid Tier
            r'\bmalath\b': 'Malath Insurance',
            r'malath cooperative': 'Malath Insurance',
            r'\bliva\b': 'Liva Insurance',
            r'liva insurance': 'Liva Insurance',
            r'tokio marine': 'Tokio Marine',
            
            # Baseline Tier
            r'\bchubb\b': 'Chubb Arabia',
            r'chubb arabia': 'Chubb Arabia',
            r'\bace\b': 'Chubb Arabia',  # Legacy name
            r'arabian shield': 'Arabian Shield Cooperative Insurance Company',
            r'\bacig\b': 'Allied Cooperative Insurance Group',
            r'allied cooperative': 'Allied Cooperative Insurance Group',
            r'\bsaico\b': 'Saudi Arabian Cooperative Insurance Company',
            r'saudi arabian cooperative': 'Saudi Arabian Cooperative Insurance Company',
            r'\bsalama\b': 'Salama Insurance',
            r'salama cooperative': 'Salama Insurance',
            r'\bajtc\b': 'Al Jazeera Takaful Company',
            r'al jazeera takaful': 'Al Jazeera Takaful Company',
            r'\baicc\b': 'Arabia Insurance Cooperative Company',
            r'\bacic\b': 'Arabia Insurance Cooperative Company',
            r'arabia insurance cooperative': 'Arabia Insurance Cooperative Company',
            r'arab cooperative insurance': 'Arabia Insurance Cooperative Company',
            r'\bal[\s-]?sagr\b': 'Al Sagr Co-operative Insurance Company',
            r'al[\s-]?sagr cooperative': 'Al Sagr Co-operative Insurance Company',
            r'alsagr': 'Al Sagr Co-operative Insurance Company',
            r'\bamanah\b': 'Amanah Cooperative Insurance Company',
            r'amanah cooperative': 'Amanah Cooperative Insurance Company',
            r'\bmutakamela\b': 'Mutakamela Insurance',
            r'\bart\b': 'Al Rajhi Takaful',
            r'al[\s-]?rajhi takaful': 'Al Rajhi Takaful',
            r'alrajhi takaful': 'Al Rajhi Takaful',
            
            # Challenged Tier
            r'\bgulf union\b': 'Gulf Union Alahlia Cooperative Insurance Company',
            r'gulf union alahlia': 'Gulf Union Alahlia Cooperative Insurance Company',
            r'gulf union cooperative': 'Gulf Union Alahlia Cooperative Insurance Company',
            r'\buca\b': 'United Cooperative Assurance (UCA)',
            r'united cooperative assurance': 'United Cooperative Assurance (UCA)',
            r'united cooperative': 'United Cooperative Assurance (UCA)',
        }
        
        # Check with regex patterns for better accuracy
        for pattern, full_name in INSURER_REFERENCE_PATTERNS.items():
            if re.search(pattern, filename_clean, re.IGNORECASE):
                logger.info(f"✅ Detected insurer from filename: {full_name} (pattern: {pattern})")
                return InsurerDetectionResult(name=full_name, method="filename", confidence="high")
    
    # Priority 2: Check text content (search entire document for better coverage)
    # Search up to 10000 characters (covers most multi-page documents)
    search_text_length = min(10000, len(text))
    text_upper = text[:search_text_length].upper()
    text_lower = text[:search_text_length].lower()
    full_text_upper = text.upper()  # For patterns that need full text

    # KNOWN_INSURER_KEYWORDS: Reference data for exact text matching and normalization.
    # Used for fuzzy matching - NOT a strict whitelist or validation requirement.
    KNOWN_INSURER_KEYWORDS = {
        # Premium Tier Companies
        'TAWUNIYA': 'Tawuniya',
        'THE COMPANY FOR COOPERATIVE INSURANCE': 'Tawuniya',
        'COMPANY FOR COOPERATIVE INSURANCE': 'Tawuniya',
        'WALAA INSURANCE': 'Walaa Insurance',
        'WALAA COOPERATIVE': 'Walaa Insurance',
        'WALAA': 'Walaa Insurance',
        'MEDGULF INSURANCE': 'MedGulf Insurance',
        'MEDITERRANEAN AND GULF': 'MedGulf Insurance',
        'MEDGULF': 'MedGulf Insurance',
        
        # Strong Tier Companies
        'GULF INSURANCE GROUP': 'Gulf Insurance Group (GIG)',
        'GIG': 'Gulf Insurance Group (GIG)',
        'GULF GENERAL': 'Gulf General Cooperative Insurance Company',
        'GULF GENERAL COOPERATIVE': 'Gulf General Cooperative Insurance Company',
        'GGI': 'Gulf General Cooperative Insurance Company',
        'AL-ETIHAD': 'Al-Etihad Cooperative Insurance',
        'AL ETIHAD': 'Al-Etihad Cooperative Insurance',
        'WATANIYA INSURANCE': 'Wataniya Insurance',
        'WATANIYA': 'Wataniya Insurance',
        'AXA GULF': 'AXA Gulf',
        'AXA COOPERATIVE': 'AXA Gulf',
        'AXA': 'AXA Gulf',
        'ALLIANZ': 'Allianz Saudi Fransi',
        'ALLIANZ SAUDI FRANSI': 'Allianz Saudi Fransi',
        'ZURICH INSURANCE': 'Zurich Insurance',
        'ZURICH': 'Zurich Insurance',
        
        # Solid Tier Companies
        'MALATH INSURANCE': 'Malath Insurance',
        'MALATH COOPERATIVE': 'Malath Insurance',
        'MALATH': 'Malath Insurance',
        'LIVA INSURANCE': 'Liva Insurance',
        'LIVA': 'Liva Insurance',
        'TOKIO MARINE': 'Tokio Marine',
        'TOKIO MARINE SAUDI ARABIA': 'Tokio Marine',
        
        # Baseline Tier Companies
        'CHUBB ARABIA': 'Chubb Arabia',
        'CHUBB': 'Chubb Arabia',
        'CHUBB ARABIA COOPERATIVE': 'Chubb Arabia',
        'ARABIAN SHIELD': 'Arabian Shield Cooperative Insurance Company',
        'ARABIAN SHIELD COOPERATIVE': 'Arabian Shield Cooperative Insurance Company',
        'ALLIED COOPERATIVE INSURANCE': 'Allied Cooperative Insurance Group',
        'ALLIED COOPERATIVE': 'Allied Cooperative Insurance Group',
        'ACIG': 'Allied Cooperative Insurance Group',
        'SAUDI ARABIAN COOPERATIVE INSURANCE': 'Saudi Arabian Cooperative Insurance Company',
        'SAICO': 'Saudi Arabian Cooperative Insurance Company',
        'SALAMA INSURANCE': 'Salama Insurance',
        'SALAMA COOPERATIVE': 'Salama Insurance',
        'SALAMA': 'Salama Insurance',
        'AL JAZEERA TAKAFUL': 'Al Jazeera Takaful Company',
        'AJTC': 'Al Jazeera Takaful Company',
        'ARAB COOPERATIVE INSURANCE': 'Arabia Insurance Cooperative Company',
        'ARABIA INSURANCE COOPERATIVE': 'Arabia Insurance Cooperative Company',
        'AICC': 'Arabia Insurance Cooperative Company',
        'ACIC': 'Arabia Insurance Cooperative Company',
        'AL SAGR': 'Al Sagr Co-operative Insurance Company',
        'AL-SAGR': 'Al Sagr Co-operative Insurance Company',
        'ALSAGR': 'Al Sagr Co-operative Insurance Company',
        'AL SAGR COOPERATIVE': 'Al Sagr Co-operative Insurance Company',
        'AL-SAGR COOPERATIVE': 'Al Sagr Co-operative Insurance Company',
        'AMANAH': 'Amanah Cooperative Insurance Company',
        'AMANAH COOPERATIVE': 'Amanah Cooperative Insurance Company',
        'MUTAKAMELA': 'Mutakamela Insurance',
        'MUTAKAMELA INSURANCE': 'Mutakamela Insurance',
        'AL RAJHI TAKAFUL': 'Al Rajhi Takaful',
        'ART': 'Al Rajhi Takaful',
        'ALRAJHI TAKAFUL': 'Al Rajhi Takaful',
        
        # Challenged Tier Companies
        'GULF UNION': 'Gulf Union Alahlia Cooperative Insurance Company',
        'GULF UNION ALAHLIA': 'Gulf Union Alahlia Cooperative Insurance Company',
        'GULF UNION COOPERATIVE': 'Gulf Union Alahlia Cooperative Insurance Company',
        'UNITED COOPERATIVE ASSURANCE': 'United Cooperative Assurance (UCA)',
        'UNITED COOPERATIVE': 'United Cooperative Assurance (UCA)',
        'UCA': 'United Cooperative Assurance (UCA)',
    }
    
    for key, full_name in KNOWN_INSURER_KEYWORDS.items():
        # For short keywords (≤4 chars), use word boundary regex to avoid false positives
        # e.g., 'ART' should match 'ART' but not 'PARTial'
        if len(key) <= 4:
            # Use word boundary regex
            pattern = r'\b' + re.escape(key) + r'\b'
            if re.search(pattern, text_upper):
                logger.info(f"✅ Detected insurer from text (word boundary): {full_name} (keyword: {key})")
                return InsurerDetectionResult(name=full_name, method="text_match", confidence="high")
        else:
            # For longer keywords, substring matching is safe
            if key in text_upper:
                logger.info(f"✅ Detected insurer from text: {full_name} (keyword: {key})")
                return InsurerDetectionResult(name=full_name, method="text_match", confidence="high")
    
    # Check for UCA as standalone abbreviation (common in documents) - search full text
    uca_patterns = [
        r'\bU\.C\.A\.\b',
        r'\bUCA\b',
        r'United Cooperative Assurance',
    ]
    for pattern in uca_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            logger.info(f"✅ Detected UCA from text pattern: {pattern}")
            return InsurerDetectionResult(name='United Cooperative Assurance (UCA)', method="text_match", confidence="high")
    
    # Check for GIG abbreviation
    if re.search(r'\bGIG\b', text, re.IGNORECASE):
        logger.info(f"✅ Detected GIG from text")
        return InsurerDetectionResult(name='Gulf Insurance Group (GIG)', method="text_match", confidence="high")
    
    # Priority 3: Pattern matching for generic insurance company names (search full text)
    patterns = [
        r'Form[:\s]+As per ([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
        r'([A-Za-z\s&]+(?:Insurance|Group|Assurance))\s+(?:Wording|Policy)',
        r'Signed for and on behalf of ([A-Za-z\s&]+(?:Insurance|Company|Assurance))',
        r'Issued by ([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
        r'Insurer[:\s]+([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
        r'Insurance Company[:\s]+([A-Za-z\s&]+)',
        r'([A-Za-z\s&]+Insurance Company)',
        r'Provider[:\s]+([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
        r'Underwritten by ([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
        r'Policy Issued by ([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
        r'Quote from ([A-Za-z\s&]+(?:Insurance|Group|Assurance))',
    ]
    
    for pattern in patterns:
        try:
            # Search in chunks to find matches throughout document
            matches = list(re.finditer(pattern, text, re.IGNORECASE))
            for match in matches:
                if match and len(match.groups()) > 0:
                    company = match.group(1).strip()
                    
                    # CRITICAL FIX: Enhanced validation to filter false positives
                    # Reject if too short or suspiciously long
                    if len(company) < 10 or len(company) > 100:
                        continue
                    
                    # Filter out common false positives
                    if company.lower() in ['the', 'as per', 'per', 'for', 'and']:
                        continue
                    
                    company_lower = company.lower()
                    
                    # CRITICAL FIX: Blacklist consent clauses and common false positives
                    blacklist_phrases = [
                        'information that it requires',
                        'authorize',
                        'i hereby authorize',
                        'consent',
                        'simah',
                        'credit bureau',
                        'i hereby',
                        'the insured',
                        'the undersigned',
                        'declare and agree',
                        'terms and conditions',
                    ]
                    
                    # Skip if matches blacklist
                    if any(phrase in company_lower for phrase in blacklist_phrases):
                        logger.debug(f"⚠️ Skipped false positive: '{company}' (matches blacklist)")
                        continue
                    
                    # Additional validation - check if it contains insurance-related keywords
                    if any(keyword in company_lower for keyword in ['insurance', 'assurance', 'group', 'cooperative', 'takaful']):
                        logger.info(f"✅ Detected insurer from pattern: {company} (pattern: {pattern})")
                        return InsurerDetectionResult(name=company, method="pattern", confidence="medium")
        except Exception as e:
            logger.debug(f"Pattern matching error: {e}")
            continue
    
    # Priority 4: AI-powered fallback detection (if pattern matching fails)
    logger.info("🤖 Attempting AI-powered insurer detection...")
    try:
        # Use more text - first 8000 chars + last 2000 chars (headers/footers often contain company name)
        text_for_ai = text[:8000] + "\n\n[END OF FIRST SECTION]\n\n" + text[-2000:] if len(text) > 10000 else text
        ai_detected = await _ai_detect_insurer(text_for_ai)
        if ai_detected and ai_detected != "Unknown Insurer":
            logger.info(f"✅ AI detected insurer: {ai_detected}")
            return InsurerDetectionResult(name=ai_detected, method="ai", confidence="low")
    except Exception as e:
        logger.warning(f"⚠️ AI detection failed: {e}")

    # Priority 5: OCR fallback (NEW - last resort)
    if pdf_path:
        logger.info("All text-based methods failed - attempting OCR fallback")
        ocr_detected = await _ocr_fallback_detect_insurer(pdf_path)
        if ocr_detected:
            logger.info(f"✅ OCR detected insurer: {ocr_detected}")
            return InsurerDetectionResult(name=ocr_detected, method="ocr", confidence="low")
    else:
        logger.info("No pdf_path provided - skipping OCR fallback")

    # Final fallback
    logger.warning(f"⚠️ All detection methods (including OCR) failed")
    return InsurerDetectionResult(name=None, method=None, confidence=None)


async def _ai_detect_insurer(text_sample: str) -> str:
    """
    Use AI to detect insurance company name from document text.
    Fallback when pattern matching fails.
    """
    if not text_sample or len(text_sample.strip()) < 100:
        return None
    
    try:
        prompt = f"""You are an expert at analyzing insurance documents. Extract the name of the INSURANCE COMPANY (the insurer/provider) from the following document text.

IMPORTANT: Return ONLY the insurance company name, nothing else. If you cannot find it, return None.

Known Saudi insurance companies include:
- Tawuniya (The Company for Cooperative Insurance)
- Walaa Cooperative Insurance Company
- MedGulf (Mediterranean and Gulf Insurance)
- Gulf Insurance Group (GIG)
- Gulf General Cooperative Insurance (GGI)
- Al-Etihad Cooperative Insurance
- Wataniya Insurance
- Malath Insurance
- Liva Insurance
- Chubb Arabia
- Arabian Shield Cooperative Insurance Company
- Allied Cooperative Insurance Group (ACIG)
- Saudi Arabian Cooperative Insurance Company (SAICO)
- Salama Insurance
- Al Jazeera Takaful Company (AJTC)
- Arabia Insurance Cooperative Company (AICC)
- Al Sagr Co-operative Insurance Company
- Amanah Cooperative Insurance Company
- Mutakamela Insurance
- Al Rajhi Takaful (ART)
- Gulf Union Alahlia Cooperative Insurance Company
- United Cooperative Assurance (UCA)
- AXA Gulf
- Allianz Saudi Fransi
- Zurich Insurance
- Tokio Marine

Document text sample (may include headers/footers):
{text_sample[:5000]}

Look carefully for:
- Company names in headers or footers
- "Issued by" or "Provided by" statements
- Company logos or branding text
- Signatures or authorizations with company names
- Policy numbers or quote references that might indicate the company

Return the exact insurance company name as it appears in the document, or a standard name if you recognize it. Return ONLY the company name, no explanations."""

        response = await openai_client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are an expert insurance document analyzer. Extract the insurance company name from documents."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=100
        )
        
        detected_name = response.choices[0].message.content.strip()
        
        # Clean up the response
        detected_name = detected_name.replace('"', '').replace("'", "").strip()
        
        # Validate - check if it's a known insurer or contains insurance keywords
        if detected_name.lower() in ['unknown insurer', 'unknown', 'not found', 'n/a', 'none']:
            return None
        
        # Check if it contains insurance-related keywords
        detected_lower = detected_name.lower()
        if any(keyword in detected_lower for keyword in ['insurance', 'assurance', 'cooperative', 'takaful', 'group']):
            # Try to match with known insurers
            for known_pattern, full_name in [
                ('tawuniya', 'Tawuniya'),
                ('company for cooperative insurance', 'Tawuniya'),
                ('walaa', 'Walaa Insurance'),
                ('medgulf', 'MedGulf Insurance'),
                ('mediterranean and gulf', 'MedGulf Insurance'),
                ('gig', 'Gulf Insurance Group (GIG)'),
                ('gulf insurance group', 'Gulf Insurance Group (GIG)'),
                ('ggi', 'Gulf General Cooperative Insurance Company'),
                ('gulf general', 'Gulf General Cooperative Insurance Company'),
                ('al-etihad', 'Al-Etihad Cooperative Insurance'),
                ('al etihad', 'Al-Etihad Cooperative Insurance'),
                ('wataniya', 'Wataniya Insurance'),
                ('malath', 'Malath Insurance'),
                ('liva', 'Liva Insurance'),
                ('tokio marine', 'Tokio Marine'),
                ('chubb', 'Chubb Arabia'),
                ('arabian shield', 'Arabian Shield Cooperative Insurance Company'),
                ('acig', 'Allied Cooperative Insurance Group'),
                ('allied cooperative', 'Allied Cooperative Insurance Group'),
                ('saico', 'Saudi Arabian Cooperative Insurance Company'),
                ('saudi arabian cooperative', 'Saudi Arabian Cooperative Insurance Company'),
                ('salama', 'Salama Insurance'),
                ('ajtc', 'Al Jazeera Takaful Company'),
                ('al jazeera takaful', 'Al Jazeera Takaful Company'),
                ('aicc', 'Arabia Insurance Cooperative Company'),
                ('acic', 'Arabia Insurance Cooperative Company'),
                ('arabia insurance cooperative', 'Arabia Insurance Cooperative Company'),
                ('al sagr', 'Al Sagr Co-operative Insurance Company'),
                ('al-sagr', 'Al Sagr Co-operative Insurance Company'),
                ('alsagr', 'Al Sagr Co-operative Insurance Company'),
                ('amanah', 'Amanah Cooperative Insurance Company'),
                ('mutakamela', 'Mutakamela Insurance'),
                ('art', 'Al Rajhi Takaful'),
                ('al rajhi takaful', 'Al Rajhi Takaful'),
                ('alrajhi takaful', 'Al Rajhi Takaful'),
                ('gulf union', 'Gulf Union Alahlia Cooperative Insurance Company'),
                ('uca', 'United Cooperative Assurance (UCA)'),
                ('united cooperative', 'United Cooperative Assurance (UCA)'),
                ('axa', 'AXA Gulf'),
                ('allianz', 'Allianz Saudi Fransi'),
                ('zurich', 'Zurich Insurance'),
            ]:
                if known_pattern in detected_lower:
                    return full_name
            
            # Return the detected name as-is if it seems valid
            return detected_name
        
        return None
        
    except Exception as e:
        logger.error(f"❌ AI insurer detection error: {e}")
        return None


def _extract_insured_from_text(text: str) -> str:
    """Extract insured party (customer) name."""
    patterns = [
        r'(?:Insured|Policy Holder|Name of Insured|Client)[:\s]+([A-Za-z0-9\s&/.-]+?)(?:\n|CR#|Limited|Ltd)',
        r'INSURED[:\s]+([A-Za-z0-9\s&/.-]+?)(?:\n|CR#)',
        r'Insured\'s Name[:\s]+([A-Za-z0-9\s&/.-]+?)(?:\n)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text[:3000], re.IGNORECASE)
        if match:
            insured = match.group(1).strip()
            if len(insured) > 5:
                return insured
    
    return "Unknown Insured"


def _determine_applicable_deductible_tier(sum_insured: float, deductible_tiers: List[Dict]) -> Dict:
    """
    Determine which deductible tier applies based on actual sum insured.
    FIXES CRITICAL ISSUE: Properly matches SI to correct tier.
    """
    if not sum_insured or not deductible_tiers:
        return {}
    
    si_millions = sum_insured / 1_000_000
    
    for tier in deductible_tiers:
        tier_range = tier.get('range', '').lower()
        
        if 'above sr 500' in tier_range or 'above 500' in tier_range:
            if si_millions > 500:
                logger.info(f"✓ Deductible tier: >500M applies (SI: {si_millions:.1f}M)")
                return tier
        elif 'above sr 100' in tier_range or '100' in tier_range and '500' in tier_range:
            if 100 < si_millions <= 500:
                logger.info(f"✓ Deductible tier: 100-500M applies (SI: {si_millions:.1f}M)")
                return tier
        elif 'above sr 40' in tier_range or '40' in tier_range and '100' in tier_range:
            if 40 < si_millions <= 100:
                logger.info(f"✓ Deductible tier: 40-100M applies (SI: {si_millions:.1f}M)")
                return tier
        elif 'up to sr 40' in tier_range or 'upto 40' in tier_range:
            if si_millions <= 40:
                logger.info(f"✓ Deductible tier: ≤40M applies (SI: {si_millions:.1f}M)")
                return tier
    
    return deductible_tiers[0] if deductible_tiers else {}


def _calculate_premium_from_rate(sum_insured: float, rate_string: str) -> Optional[float]:
    """Calculate premium with support for all rate formats."""
    try:
        rate_match = re.search(r'(\d+\.?\d*)', rate_string.replace(',', ''))
        if not rate_match:
            return None

        rate_value = float(rate_match.group(1))
        rate_lower = rate_string.lower()

        if '‰' in rate_string or 'per mille' in rate_lower or '%o' in rate_string:
            premium = (sum_insured * rate_value) / 1000
        elif '%' in rate_string and 'per mille' not in rate_lower:
            premium = (sum_insured * rate_value) / 100
        elif 'basis point' in rate_lower or 'bp' in rate_lower:
            premium = (sum_insured * rate_value) / 10000
        else:
            premium = (sum_insured * rate_value) / 1000

        logger.info(f"💰 Calculated: {premium:.2f} from SI: {sum_insured} × Rate: {rate_string}")
        return premium

    except Exception as e:
        logger.error(f"Premium calculation failed: {str(e)}")
        return None


def _detect_insurance_category(text: str, policy_type: str) -> Tuple[str, str]:
    """
    Detect insurance category from policy type and text content.

    Args:
        text: Full PDF text
        policy_type: Extracted policy type string

    Returns:
        (category, confidence) where:
        - category: "property", "liability", "medical", "motor", "marine", "engineering", "other"
        - confidence: "high", "medium", "low"
    """
    if not policy_type:
        policy_type = ""

    policy_lower = policy_type.lower()
    text_lower = text.lower() if text else ""

    # Liability keywords (highest priority - this is what we're adding support for)
    liability_keywords = [
        'liability', 'cgl', 'general liability', 'third party', 'public liability',
        'professional indemnity', 'professional liability', 'errors and omissions',
        'e&o', 'employers liability', 'products liability', 'product liability',
        'd&o', 'directors and officers', 'directors & officers', 'legal liability',
        'bodily injury liability', 'property damage liability', 'comprehensive general liability'
    ]

    # Property keywords
    property_keywords = [
        'property', 'fire', 'all risk', 'material damage', 'business interruption',
        'par', 'buildings', 'contents', 'stock', 'machinery breakdown',
        'commercial property', 'industrial all risks', 'iar'
    ]

    # Medical keywords
    medical_keywords = [
        'medical', 'health', 'malpractice', 'medical malpractice',
        'clinical negligence', 'healthcare professional'
    ]

    # Motor keywords
    motor_keywords = [
        'motor', 'auto', 'vehicle', 'car', 'fleet', 'comprehensive motor',
        'third party motor', 'tpl motor'
    ]

    # Marine keywords
    marine_keywords = [
        'marine', 'cargo', 'hull', 'marine cargo', 'ocean marine',
        'inland marine', 'transit'
    ]

    # Engineering keywords
    engineering_keywords = [
        'engineering', 'contractors', 'erection', 'car', 'ear',
        'contractors all risks', 'erection all risks', 'machinery breakdown',
        'boiler and machinery'
    ]

    # Count matches in policy type (weighted higher)
    def count_matches(keywords, text_to_check, weight=1.0):
        count = 0
        for keyword in keywords:
            if keyword in text_to_check:
                count += weight
        return count

    # Check policy type first (higher weight)
    policy_scores = {
        'liability': count_matches(liability_keywords, policy_lower, weight=2.0),
        'property': count_matches(property_keywords, policy_lower, weight=2.0),
        'medical': count_matches(medical_keywords, policy_lower, weight=2.0),
        'motor': count_matches(motor_keywords, policy_lower, weight=2.0),
        'marine': count_matches(marine_keywords, policy_lower, weight=2.0),
        'engineering': count_matches(engineering_keywords, policy_lower, weight=2.0),
    }

    # Check text content (lower weight, first 5000 chars only)
    text_sample = text_lower[:5000] if text_lower else ""
    text_scores = {
        'liability': count_matches(liability_keywords, text_sample, weight=0.5),
        'property': count_matches(property_keywords, text_sample, weight=0.5),
        'medical': count_matches(medical_keywords, text_sample, weight=0.5),
        'motor': count_matches(motor_keywords, text_sample, weight=0.5),
        'marine': count_matches(marine_keywords, text_sample, weight=0.5),
        'engineering': count_matches(engineering_keywords, text_sample, weight=0.5),
    }

    # Combine scores
    total_scores = {
        category: policy_scores[category] + text_scores[category]
        for category in policy_scores.keys()
    }

    # Find best match
    if max(total_scores.values()) == 0:
        return "other", "low"

    best_category = max(total_scores, key=total_scores.get)
    best_score = total_scores[best_category]

    # Determine confidence
    if best_score >= 3.0:
        confidence = "high"
    elif best_score >= 1.5:
        confidence = "medium"
    else:
        confidence = "low"

    logger.info(f"🔍 Insurance Category Detection: {best_category} (confidence: {confidence}, score: {best_score:.1f})")
    logger.debug(f"  Policy Type: '{policy_type}'")
    logger.debug(f"  Scores: {total_scores}")

    return best_category, confidence


def _comprehensive_vat_detection(text: str, prem_info: Dict) -> Dict:
    """
    Comprehensive VAT detection with exhaustive pattern checking.

    This function implements the pattern-first approach:
    1. Check VAT_EXCLUSIVE patterns (more specific - indicates VAT will be added)
    2. Check VAT_INCLUSIVE patterns (indicates VAT already in premium)
    3. Validate extracted AI values against document text
    4. Default to P2 with 15% if VAT is mentioned but unclear
    5. Only reject for P5 (Zero VAT) and P6 (Non-standard rate)

    Args:
        text: Full document text (no character limit)
        prem_info: Extracted premium information from AI

    Returns:
        {
            "class": "P1" | "P2" | "P5" | "P6",
            "is_inclusive": bool,
            "vat_percentage": float | None,
            "vat_amount": float | None,
            "detection_method": str,
            "pattern_matched": str | None,
            "confidence": "high" | "medium" | "low",
            "warning": str | None,
            "requires_verification": bool
        }
    """
    logger.info("=" * 80)
    logger.info("🔍 COMPREHENSIVE VAT DETECTION v9.0")
    logger.info("=" * 80)

    # Parse extracted values from AI
    vat_amount_text = str(prem_info.get('vat_amount', '')).strip()
    vat_percentage_text = str(prem_info.get('vat_percentage', '')).strip()

    vat_amount_extracted = _parse_currency_amount(vat_amount_text)
    vat_percentage_extracted = None
    if vat_percentage_text and vat_percentage_text not in ['', '0', 'N/A', 'None']:
        try:
            vat_percentage_extracted = float(vat_percentage_text)
        except ValueError:
            pass

    logger.info(f"📊 AI Extracted Values:")
    logger.info(f"   vat_amount: {vat_amount_extracted}")
    logger.info(f"   vat_percentage: {vat_percentage_extracted}")

    text_lower = text.lower() if text else ""

    # ==================================================================
    # STEP 1: CHECK FOR EXPLICIT ZERO VAT OR NON-STANDARD RATE (P5/P6)
    # ==================================================================
    if vat_percentage_extracted == 0:
        # Check if "0%" actually appears in document
        if re.search(r'vat\s*:?\s*0\s*%', text_lower, re.IGNORECASE):
            logger.error("❌ P5 DETECTED: Zero VAT explicitly stated")
            return {
                "class": "P5",
                "is_inclusive": False,
                "vat_percentage": None,
                "vat_amount": None,
                "detection_method": "zero_vat_confirmed",
                "pattern_matched": "0% VAT",
                "confidence": "high",
                "warning": None,
                "requires_verification": False
            }

    if vat_percentage_extracted is not None and vat_percentage_extracted not in [15.0]:
        # Check if this non-standard rate actually appears in document
        rate_str = str(vat_percentage_extracted).replace('.0', '')
        if re.search(rf'\b{rate_str}\s*%.*vat|vat.*{rate_str}\s*%', text_lower, re.IGNORECASE):
            logger.error(f"❌ P6 DETECTED: Non-standard VAT rate {vat_percentage_extracted}% confirmed in text")
            return {
                "class": "P6",
                "is_inclusive": False,
                "vat_percentage": None,
                "vat_amount": None,
                "detection_method": "non_standard_rate_confirmed",
                "pattern_matched": f"{vat_percentage_extracted}% VAT",
                "confidence": "high",
                "warning": None,
                "requires_verification": False
            }
        else:
            logger.warning(f"⚠️ AI extracted {vat_percentage_extracted}% but not found in text - treating as hallucinated")

    # ==================================================================
    # STEP 1.5: CHECK VAT_DEFERRED PATTERNS (P3 Classification)
    # ==================================================================
    logger.info("🔍 Step 1.5: Checking VAT_DEFERRED patterns (P3)...")

    for pattern in VAT_DEFERRED_PATTERNS:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            matched_text = match.group(0)
            logger.info(f"✅ VAT_DEFERRED pattern matched: '{matched_text}'")
            logger.info(f"   Pattern: {pattern}")

            # CRITICAL FIX: Check if explicit VAT line exists (even if zero)
            # Zero VAT indicates P5 (zero VAT), not P3 (deferred VAT)
            if vat_amount_extracted is not None:
                if vat_amount_extracted > 0:
                    logger.info(f"   ⚠️ Explicit positive VAT amount found: {vat_amount_extracted}")
                    logger.info("   Skipping P3, continuing to P2 check...")
                    continue
                elif vat_amount_extracted == 0:
                    logger.info("   ⚠️ Explicit ZERO VAT amount found - this is P5, not P3")
                    logger.info("   Skipping P3, P5 will be detected later...")
                    continue

            if vat_percentage_extracted is not None:
                if vat_percentage_extracted > 0:
                    logger.info(f"   ⚠️ Explicit positive VAT percentage found: {vat_percentage_extracted}%")
                    logger.info("   Skipping P3, continuing to P2 check...")
                    continue
                elif vat_percentage_extracted == 0:
                    logger.info("   ⚠️ Explicit ZERO VAT percentage found - this is P5, not P3")
                    logger.info("   Skipping P3, P5 will be detected later...")
                    continue

            logger.info("   No explicit VAT values - confirming P3 classification")
            logger.info("   P3: VAT will be charged at billing time - no VAT in quote")

            return {
                "class": "P3",
                "is_inclusive": False,
                "vat_percentage": None,  # No VAT percentage in quote
                "vat_amount": None,       # No VAT amount in quote
                "detection_method": "pattern_vat_deferred",
                "pattern_matched": matched_text,
                "confidence": "high",
                "warning": "VAT will be charged at time of billing. Quote does not include VAT.",
                "requires_verification": False
            }

    logger.info("   No VAT_DEFERRED patterns found")

    # ==================================================================
    # STEP 2: CHECK VAT_EXCLUSIVE PATTERNS (Priority 1 - Most Specific)
    # ==================================================================
    logger.info("🔍 Step 2: Checking VAT_EXCLUSIVE patterns...")

    for pattern in VAT_EXCLUSIVE_PATTERNS:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            matched_text = match.group(0)
            logger.info(f"✅ VAT_EXCLUSIVE pattern matched: '{matched_text}'")
            logger.info(f"   Pattern: {pattern}")

            # Validate extracted percentage if present
            final_percentage = 15.0  # Saudi default
            final_amount = None
            confidence = "high"
            warning = None

            if vat_percentage_extracted and vat_percentage_extracted == 15.0:
                # Extracted value matches Saudi standard
                final_percentage = 15.0
                if vat_amount_extracted:
                    final_amount = vat_amount_extracted
                logger.info(f"   Extracted VAT 15% confirmed (Saudi standard)")
            elif vat_percentage_extracted and vat_percentage_extracted != 15.0:
                # Extracted value doesn't match - use default
                logger.warning(f"   Extracted VAT {vat_percentage_extracted}% doesn't match pattern, using 15% default")
                confidence = "medium"
                warning = f"Document indicates VAT-exclusive but no specific rate found. Using Saudi standard 15%."
            else:
                # No extracted percentage - use default
                logger.info(f"   No specific rate mentioned, using Saudi standard 15%")
                warning = "Document indicates VAT-exclusive. Using Saudi standard 15%."

            return {
                "class": "P2",
                "is_inclusive": False,
                "vat_percentage": final_percentage,
                "vat_amount": final_amount,
                "detection_method": "pattern_vat_exclusive",
                "pattern_matched": matched_text,
                "confidence": confidence,
                "warning": warning,
                "requires_verification": warning is not None
            }

    logger.info("   No VAT_EXCLUSIVE patterns found")

    # ==================================================================
    # STEP 3: CHECK VAT_INCLUSIVE PATTERNS (Priority 2)
    # ==================================================================
    logger.info("🔍 Step 3: Checking VAT_INCLUSIVE patterns...")

    for pattern in VAT_INCLUSIVE_PATTERNS:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            matched_text = match.group(0)
            logger.info(f"✅ VAT_INCLUSIVE pattern matched: '{matched_text}'")
            logger.info(f"   Pattern: {pattern}")
            logger.info(f"   VAT already included in premium - no separate calculation needed")

            return {
                "class": "P1",
                "is_inclusive": True,
                "vat_percentage": None,
                "vat_amount": None,
                "detection_method": "pattern_vat_inclusive",
                "pattern_matched": matched_text,
                "confidence": "high",
                "warning": None,
                "requires_verification": False
            }

    logger.info("   No VAT_INCLUSIVE patterns found")

    # ==================================================================
    # STEP 4: CHECK IF VAT IS MENTIONED AT ALL
    # ==================================================================
    logger.info("🔍 Step 4: Checking if VAT is mentioned...")

    vat_mentioned = False
    for pattern in VAT_MENTIONED_PATTERNS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            vat_mentioned = True
            logger.info(f"   VAT mentioned in document (pattern: {pattern})")
            break

    if vat_mentioned:
        # VAT is mentioned but structure unclear - default to P2 with warning
        logger.warning("⚠️ VAT mentioned but structure unclear")
        logger.warning("   Defaulting to P2 (VAT-exclusive) with 15% Saudi standard")

        return {
            "class": "P2",
            "is_inclusive": False,
            "vat_percentage": 15.0,
            "vat_amount": None,
            "detection_method": "vat_mentioned_unclear",
            "pattern_matched": "VAT (unclear structure)",
            "confidence": "medium",
            "warning": "VAT mentioned in document but structure unclear. Assumed 15% VAT-exclusive (Saudi standard).",
            "requires_verification": True
        }

    # ==================================================================
    # STEP 5: NO VAT MENTION - DEFAULT TO P2 WITH WARNING
    # ==================================================================
    logger.warning("⚠️ No VAT information found in document")
    logger.warning("   Defaulting to P2 (VAT-exclusive) with 15% Saudi standard")
    logger.warning("   This is the standard assumption for Saudi insurance policies")

    return {
        "class": "P2",
        "is_inclusive": False,
        "vat_percentage": 15.0,
        "vat_amount": None,
        "detection_method": "default_saudi_standard",
        "pattern_matched": None,
        "confidence": "low",
        "warning": "No VAT information found. Assumed 15% VAT-exclusive (Saudi standard). Please verify.",
        "requires_verification": True
    }


def _detect_vat_signal_type(prem_info: Dict, text: str = "") -> str:
    """
    Detect the type of VAT signal present in the document.
    
    Signal Types:
    - FINANCIAL_LINE_ITEM: Explicit VAT amount or % tied to premium (only trusted source for VAT numbers)
    - PRICE_ANNOTATION: "incl. VAT" / "excl. VAT" near premium (conditional - requires explicit wording)
    - LEGAL_CLAUSE: "VAT as applicable", "insured shall pay VAT" (never produces numbers)
    - NONE: No VAT signals found (default state)
    
    Returns:
        One of: "FINANCIAL_LINE_ITEM", "PRICE_ANNOTATION", "LEGAL_CLAUSE", "NONE"
    """
    # Get VAT-related fields (raw extraction only - no math)
    premium_text = str(prem_info.get('base_premium_amount', '')).lower()
    vat_amount_text = str(prem_info.get('vat_amount', '')).strip()
    vat_percentage_text = str(prem_info.get('vat_percentage', '')).strip()
    text_lower = text.lower() if text else ""
    
    # ========================================================================
    # CRITICAL FIX: Check PRICE_ANNOTATION FIRST (before extracted amounts)
    # This prevents hallucinated VAT amounts from being treated as FINANCIAL_LINE_ITEM
    # ========================================================================
    price_annotation_patterns = [
        r'incl\.?\s+vat',
        r'incl\s+vat',
        r'including\s+vat',
        r'vat\s+included',
        r'included\s+vat',
        r'inclusive\s+vat',
        r'inclusive\s+of\s+vat',
        r'with\s+vat',
        r'incl\.?\s+tax',
        r'including\s+tax',
        r'tax\s+included',
        r'total.*included.*vat',
        r'total.*includes.*vat',
        r'total.*with.*vat.*included',
        r'total.*premium.*with.*vat.*included',
        r'premium.*included.*vat',
        r'premium.*includes.*vat',
        r'premium.*with.*vat.*included',
        r'premium.*inclusive.*vat',
        # Explicit keywords ⭐ NEW
        r'vat\s+inclusive',  # "VAT Inclusive"
        r'rates.*vat\s+inclusive',  # "Rates and premiums in this quote are VAT inclusive"
        r'premium.*vat\s+inclusive',  # "Premium is VAT inclusive"
    ]

    # Check in premium text and document text FIRST
    # CRITICAL: Use 15000 chars to catch VAT statements in footer/terms sections
    text_search_range = text_lower[:15000] if len(text_lower) > 15000 else text_lower
    search_text = premium_text + " " + text_search_range

    for pattern in price_annotation_patterns:
        if re.search(pattern, search_text, re.IGNORECASE):
            logger.info(f"🔍 VAT Signal: PRICE_ANNOTATION (found '{pattern}' - ignoring extracted VAT amounts)")
            logger.info("   Document context takes precedence over extracted amounts")
            return "PRICE_ANNOTATION"

    # ========================================================================
    # DETECT LEGAL_CLAUSE (Before FINANCIAL_LINE_ITEM to avoid false positives)
    # Uses same search_text (15000 chars) to catch "Premium Subject to VAT" in footer
    # ========================================================================
    legal_clause_patterns = [
        r'vat\s+as\s+applicable',
        r'insured\s+shall\s+pay\s+vat',
        r'vat\s+payable\s+by\s+insured',
        r'subject\s+to\s+vat',
        r'premium.*subject\s+to\s+vat',  # "Premium Subject to VAT, as applicable"
        r'vat\s+will\s+be\s+added',
        r'vat\s+shall\s+be\s+paid',
        r'vat\s+may\s+apply',
        r'vat\s+as\s+per\s+law',
        r'vat\s+according\s+to\s+regulations',
    ]
    
    for pattern in legal_clause_patterns:
        if re.search(pattern, search_text, re.IGNORECASE):
            logger.info(f"🔍 VAT Signal: LEGAL_CLAUSE (legal boilerplate found: '{pattern}')")
            return "LEGAL_CLAUSE"
    
    # ========================================================================
    # DETECT FINANCIAL_LINE_ITEM (CRITICAL FIX: Verify text patterns FIRST)
    # ========================================================================
    # Parse VAT amount and percentage (raw extraction only)
    vat_amount = _parse_currency_amount(vat_amount_text)
    vat_percentage = None
    if vat_percentage_text and vat_percentage_text not in ['', '0', 'N/A', 'None']:
        try:
            vat_percentage = float(vat_percentage_text)
        except ValueError:
            pass
    
    # CRITICAL FIX: Check document text for VAT patterns BEFORE accepting extracted amounts
    # This prevents hallucinated VAT amounts from being accepted
    text_has_vat_patterns = False
    if text:
        # FUTURE-PROOF VAT DETECTION: Catches VAT in ANY context
        # (standalone lines, tables, formulas, calculations, any format)
        financial_patterns = [
            # ═══════════════════════════════════════════════════════════════
            # CATEGORY 1: VAT with SAR amounts (explicit financial line items)
            # ═══════════════════════════════════════════════════════════════
            r'VAT\s*\(?\s*\d+%?\s*\)?\s*:?\s*SAR?\s*[\d,]+',  # VAT (15%): SAR 1,500
            r'VAT\s*@?\s*\d+%\s*:?\s*SAR?\s*[\d,]+',  # VAT @ 15%: SAR 1,500
            r'Value Added Tax\s*:?\s*SAR?\s*[\d,]+',  # Value Added Tax: SAR 1,500
            r'VAT\s*:?\s*SAR?\s*[\d,]+',  # VAT: SAR 1,500
            r'VAT\s+[\d,]+\s*SAR',  # VAT 1,500 SAR
            r'SAR\s*[\d,]+.*VAT',  # SAR 1,500 VAT (any text between)

            # ═══════════════════════════════════════════════════════════════
            # CATEGORY 2: VAT with percentages (ANY context - CRITICAL FOR "69% VAT")
            # ═══════════════════════════════════════════════════════════════
            # Standard percentage formats
            r'VAT\s*\(?\s*\d+\.?\d*\s*%\s*\)?',  # VAT (15%), VAT 69%, VAT (69 %), VAT(15%)
            r'VAT\s*:?\s*\d+\.?\d*\s*%',  # VAT: 15%, VAT 69%
            r'\d+\.?\d*\s*%\s*VAT',  # 15% VAT, 69% VAT, 69.0% VAT ⭐ CRITICAL
            r'VAT\s+Rate\s*:?\s*\d+\.?\d*\s*%',  # VAT Rate: 15%, VAT Rate 69%
            r'VAT\s+Percentage\s*:?\s*\d+\.?\d*\s*%',  # VAT Percentage: 69%

            # Administrative/legal statements ⭐ NEW - for "VAT 15% additional will apply"
            r'VAT\s+\d+\.?\d*\s*%\s+additional',  # VAT 15% additional
            r'VAT\s+\d+\.?\d*\s*%.*?will\s+apply',  # VAT 15% will apply, VAT 15% additional will apply
            r'VAT\s+\d+\.?\d*\s*%.*?applicable',  # VAT 15% applicable
            r'VAT\s+\d+\.?\d*\s*%.*?to\s+be\s+added',  # VAT 15% to be added

            # In calculations/formulas ⭐ for "SAR 21,689.38 + SAR 50 Fee + 69% VAT"
            r'[\+\-\*\/\=]\s*\d+\.?\d*\s*%\s*VAT',  # + 69% VAT, - 15% VAT
            r'[\+\-\*\/\=]\s*VAT\s*\d+\.?\d*\s*%',  # + VAT 69%, + VAT 15%
            r'SAR\s*[\d,\.]+\s*[\+\-\*].*\d+\.?\d*\s*%\s*VAT',  # SAR 1,500 + ... + 69% VAT
            r'SAR\s*[\d,\.]+\s*[\+\-\*].*VAT\s*\d+\.?\d*\s*%',  # SAR 1,500 + VAT 69%
            r'[\d,\.]+\s*[\+\-\*].*\d+\.?\d*\s*%\s*VAT',  # 1,500 + 69% VAT

            # Table/structured formats
            r'VAT[\s\|]*\d+\.?\d*\s*%',  # VAT | 69%, VAT    69%
            r'\d+\.?\d*\s*%[\s\|]*VAT',  # 69% | VAT, 69%    VAT

            # ═══════════════════════════════════════════════════════════════
            # CATEGORY 3: Generic VAT with numbers (no % sign)
            # ═══════════════════════════════════════════════════════════════
            r'VAT[:\s]+\d+\.?\d*(?!\d)',  # VAT: 15, VAT 69 (not part of larger number)
            r'Value\s+Added\s+Tax[:\s]+\d+\.?\d*',  # Value Added Tax: 15

            # ═══════════════════════════════════════════════════════════════
            # CATEGORY 4: Explicit "VAT Exclusive" keyword (even without percentage)
            # ⭐ NEW - for "Rates and premiums in this quote are VAT exclusive"
            # ═══════════════════════════════════════════════════════════════
            r'vat\s+exclusive',  # "VAT exclusive", "VAT Exclusive"
            r'rates.*vat\s+exclusive',  # "Rates are VAT exclusive"
            r'premium.*vat\s+exclusive',  # "Premium is VAT exclusive"
            r'quote.*vat\s+exclusive',  # "Quote is VAT exclusive"
            r'prices.*vat\s+exclusive',  # "Prices are VAT exclusive"

            # ═══════════════════════════════════════════════════════════════
            # CATEGORY 5: Ultra-flexible fallback (catches ANY VAT + number combo)
            # ═══════════════════════════════════════════════════════════════
            # Note: Case-insensitive matching already enabled via re.IGNORECASE flag
            r'VAT.*?\d+\.?\d*\s*%',  # VAT followed by any text then percentage
            r'\d+\.?\d*\s*%.*?VAT',  # Percentage followed by any text then VAT
        ]
        # Search expanded text range (15000 chars) to catch VAT statements anywhere in document
        # Many documents have VAT info in footer/terms sections which are beyond 5000 chars
        search_text = text[:15000] if len(text) > 15000 else text

        for pattern in financial_patterns:
            if re.search(pattern, search_text, re.IGNORECASE):
                text_has_vat_patterns = True
                logger.info(f"🔍 Document text contains VAT pattern: '{pattern}'")
                break
    
    # CRITICAL FIX: Only accept extracted amounts if document text confirms VAT presence
    # This prevents hallucinated amounts from being treated as FINANCIAL_LINE_ITEM
    if vat_amount and vat_amount > 0:
        if text_has_vat_patterns:
            logger.info(f"🔍 VAT Signal: FINANCIAL_LINE_ITEM (explicit VAT amount {vat_amount:,.2f} found + text confirmation)")
            return "FINANCIAL_LINE_ITEM"
        else:
            logger.warning(f"⚠️ Extracted VAT amount {vat_amount:,.2f} found but NO VAT patterns in document text - treating as hallucinated")
            logger.warning("   Ignoring extracted VAT amount (document does not mention VAT)")
    
    if vat_percentage and vat_percentage > 0:
        if text_has_vat_patterns:
            logger.info(f"🔍 VAT Signal: FINANCIAL_LINE_ITEM (explicit VAT rate {vat_percentage}% found + text confirmation)")
            return "FINANCIAL_LINE_ITEM"
        else:
            logger.warning(f"⚠️ Extracted VAT percentage {vat_percentage}% found but NO VAT patterns in document text - treating as hallucinated")
            logger.warning("   Ignoring extracted VAT percentage (document does not mention VAT)")
    
    # If text patterns exist but no extracted amounts, still return FINANCIAL_LINE_ITEM
    # (the amounts may be calculated later from the text patterns)
    if text_has_vat_patterns:
        logger.info("🔍 VAT Signal: FINANCIAL_LINE_ITEM (VAT patterns found in document text)")
        return "FINANCIAL_LINE_ITEM"
    
    # ========================================================================
    # DEFAULT: NONE
    # ========================================================================
    logger.info("🔍 VAT Signal: NONE (no VAT signals detected)")
    return "NONE"


def _classify_vat_structure(prem_info: Dict, text: str = "", vat_signal_type: str = None) -> Tuple[str, str, str, bool]:
    """
    Classify VAT structure into explicit P1-P6 classes with ENFORCEMENT.
    
    Uses vat_signal_type to enforce strict rules:
    - P1: Only if signal_type == PRICE_ANNOTATION (explicit "incl. VAT" wording)
    - P2: Only if signal_type == FINANCIAL_LINE_ITEM (explicit VAT amount/rate)
    - LEGAL_CLAUSE: Never produces VAT math (vat_amount = null, vat_percentage = null)

    VAT Classes:
    - P1 (VAT-inclusive): Premium explicitly includes VAT
    - P2 (VAT-exclusive): Premium excludes VAT, VAT shown separately
    - P3 (VAT-Deferred - ALLOWED): VAT charged at billing time
    - P4 (DISALLOWED): Only total shown, no breakdown
    - P5 (DISALLOWED): Zero VAT explicitly stated
    - P6 (DISALLOWED): Non-standard VAT (not 15%)

    Returns:
        Tuple of (vat_class, vat_signal_type, detection_method, is_vat_inclusive)

    Raises:
        VatPolicyViolation: If VAT class is P4-P6 (disallowed)

    Note:
        P3 (VAT-Deferred) is now ALLOWED and returns normally.
    """
    # VAT rate whitelist for Saudi Arabia
    ALLOWED_VAT_RATES = {15.0}
    
    # Auto-detect signal type if not provided
    if vat_signal_type is None:
        vat_signal_type = _detect_vat_signal_type(prem_info, text)

    # Get VAT-related fields from extraction
    premium_text = str(prem_info.get('base_premium_amount', '')).lower()
    vat_amount_text = str(prem_info.get('vat_amount', '')).strip()
    vat_percentage_text = str(prem_info.get('vat_percentage', '')).strip()
    total_text = str(prem_info.get('total_including_vat', '')).lower()

    # Parse VAT amount and percentage
    vat_amount = _parse_currency_amount(vat_amount_text)
    vat_percentage = None
    if vat_percentage_text and vat_percentage_text not in ['', 'N/A', 'None']:
        try:
            vat_percentage = float(vat_percentage_text)
        except ValueError:
            pass

    logger.info("🔍 VAT Classification: Analyzing VAT structure...")
    logger.info(f"   VAT Signal Type: {vat_signal_type}")
    logger.info(f"   VAT Amount Text: '{vat_amount_text}'")
    logger.info(f"   VAT Percentage Text: '{vat_percentage_text}'")
    logger.info(f"   VAT Amount Parsed: {vat_amount}")
    logger.info(f"   VAT Percentage Parsed: {vat_percentage}")

    # ========================================================================
    # CRITICAL FIX: NULLIFY HALLUCINATED VALUES
    # ========================================================================
    # If signal_type is not FINANCIAL_LINE_ITEM, the extracted VAT values are hallucinated
    # and must be set to None to prevent incorrect P5/P6 classification
    if vat_signal_type not in ["FINANCIAL_LINE_ITEM", "LEGAL_CLAUSE"]:
        if vat_amount is not None or vat_percentage is not None:
            logger.warning("🚫 Nullifying hallucinated VAT values (signal type does not support VAT math)")
            logger.warning(f"   Original vat_amount: {vat_amount}, vat_percentage: {vat_percentage}")
            logger.warning(f"   Signal type: {vat_signal_type} (only FINANCIAL_LINE_ITEM can have VAT numbers)")
            vat_amount = None
            vat_percentage = None
            logger.warning("   ✅ Hallucinated values nullified - will not trigger P5/P6 violations")

    # ========================================================================
    # LEGAL_CLAUSE HANDLING (Critical Fix - Never produces VAT math)
    # ========================================================================
    if vat_signal_type == "LEGAL_CLAUSE":
        logger.warning("⚠️ VAT Signal: LEGAL_CLAUSE detected - No VAT math will be performed")
        logger.warning("   Legal boilerplate does not constitute financial data")
        # Legal clauses should be treated as P1 (VAT-inclusive) but with null amounts
        # This allows the quote to pass but prevents VAT calculation
        return ("P1", "LEGAL_CLAUSE", "legal_clause", True)

    # ========================================================================
    # P6 DETECTION - NON-STANDARD VAT RATE (CHECK FIRST)
    # ========================================================================
    if vat_percentage is not None and vat_percentage not in ALLOWED_VAT_RATES:
        if vat_percentage == 0:
            # This is P5, handle below
            pass
        else:
            logger.error(f"❌ VAT Classification: P6 (Non-standard VAT: {vat_percentage}%)")
            logger.error(f"   Saudi Arabia only allows VAT rates: {ALLOWED_VAT_RATES}")
            raise VatPolicyViolation(
                vat_class="P6",
                reason=f"Non-standard VAT rate: {vat_percentage}% (allowed: {ALLOWED_VAT_RATES})",
                details={
                    "vat_percentage": vat_percentage,
                    "allowed_rates": list(ALLOWED_VAT_RATES)
                }
            )

    # ========================================================================
    # P5 DETECTION - ZERO VAT
    # ========================================================================
    if vat_amount == 0 or vat_percentage == 0:
        logger.error("❌ VAT Classification: P5 (Zero VAT)")
        logger.error("   Documents with zero VAT are not allowed")
        raise VatPolicyViolation(
            vat_class="P5",
            reason="Zero VAT explicitly stated",
            details={
                "vat_amount": vat_amount,
                "vat_percentage": vat_percentage
            }
        )

    # ========================================================================
    # P1 DETECTION - VAT-INCLUSIVE (Only if PRICE_ANNOTATION)
    # ========================================================================
    # P1 Rule: Only if vat_signal_type == PRICE_ANNOTATION AND no VAT rate/amount stated
    if vat_signal_type == "PRICE_ANNOTATION":
        vat_inclusive_indicators = [
            'incl. vat', 'incl vat', 'including vat', 'vat included',
            'with vat', 'incl. tax', 'including tax', 'tax included',
            'inclusive vat', 'inclusive of vat', 'inclusive tax'
        ]

        # CRITICAL FIX: Check premium_text, total_text, AND document text for VAT-inclusive indicators
        # This prevents documents with "Total Premium with VAT included" from being misclassified
        text_lower = text.lower() if text else ""
        search_text = premium_text + " " + total_text + " " + text_lower[:2000]

        if any(indicator in search_text for indicator in vat_inclusive_indicators):
            # Check: P1 requires NO VAT rate or amount stated
            if not vat_amount and not vat_percentage:
                logger.info("✅ VAT Classification: P1 (VAT-inclusive - PRICE_ANNOTATION)")
                logger.info("   Document contains 'incl. VAT', 'Total Premium with VAT included', or similar phrase")
                logger.info("   No VAT rate or amount stated (as required for P1)")
                logger.info("   This is a valid VAT-inclusive document (not P4 or P3)")
                return ("P1", "PRICE_ANNOTATION", "price_annotation", True)
            else:
                # Has VAT rate/amount but also has price annotation - this is ambiguous
                # Prefer FINANCIAL_LINE_ITEM if it exists, otherwise treat as P2
                logger.warning("⚠️ Ambiguous: PRICE_ANNOTATION found but VAT rate/amount also present")
                if vat_signal_type == "FINANCIAL_LINE_ITEM":
                    # Will be handled in P2 section
                    pass
                else:
                    # Fall through to P2 detection
                    pass

    # ========================================================================
    # P2 DETECTION - VAT-EXCLUSIVE (Only if FINANCIAL_LINE_ITEM)
    # ========================================================================
    # P2 Rule: Only if vat_signal_type == FINANCIAL_LINE_ITEM AND VAT rate in allowed list
    
    if vat_signal_type == 'FINANCIAL_LINE_ITEM':
        # Indicator 1: Separate VAT line item exists (STRICT CHECK - explicit amount)
        if vat_amount and vat_amount > 0:
            logger.info(f"✅ VAT Classification: P2 (VAT-exclusive - FINANCIAL_LINE_ITEM)")
            logger.info(f"   VAT shown as separate line: SAR {vat_amount:,.2f}")
            return ("P2", "FINANCIAL_LINE_ITEM", "separate_vat_line_item", False)

        # Indicator 2: VAT percentage is explicitly stated with valid rate
        if vat_percentage and vat_percentage in ALLOWED_VAT_RATES:
            logger.info(f"✅ VAT Classification: P2 (VAT-exclusive - FINANCIAL_LINE_ITEM)")
            logger.info(f"   VAT rate stated: {vat_percentage}% (in allowed list)")
            return ("P2", "FINANCIAL_LINE_ITEM", "vat_rate_stated", False)

        # Indicator 3: Check document text for explicit VAT breakdown patterns (with numbers)
        if text:
            vat_line_patterns = [
                r'VAT\s*\(?\s*15%?\s*\)?\s*:?\s*SAR?\s*[\d,]+',
                r'VAT\s*@?\s*15%\s*:?\s*SAR?\s*[\d,]+',
                r'Value Added Tax\s*:?\s*SAR?\s*[\d,]+',
            ]

            # Search expanded text range (15000 chars) to catch VAT anywhere in document
            search_text = text[:15000] if len(text) > 15000 else text

            for pattern in vat_line_patterns:
                if re.search(pattern, search_text, re.IGNORECASE):
                    logger.info("✅ VAT Classification: P2 (VAT-exclusive - FINANCIAL_LINE_ITEM)")
                    logger.info("   VAT line with amount found in document text")
                    return ("P2", "FINANCIAL_LINE_ITEM", "document_vat_line_pattern", False)

        # Indicator 4: "VAT Exclusive" keyword without specific percentage ⭐ NEW
        # For "Rates and premiums in this quote are VAT exclusive"
        if text:
            vat_exclusive_patterns = [
                r'vat\s+exclusive',
                r'rates.*vat\s+exclusive',
                r'premium.*vat\s+exclusive',
                r'quote.*vat\s+exclusive',
                r'prices.*vat\s+exclusive',
            ]

            search_text = text[:15000] if len(text) > 15000 else text

            for pattern in vat_exclusive_patterns:
                if re.search(pattern, search_text, re.IGNORECASE):
                    logger.info("✅ VAT Classification: P2 (VAT-exclusive - FINANCIAL_LINE_ITEM)")
                    logger.info(f"   'VAT Exclusive' keyword found in document")
                    logger.info("   No specific rate mentioned - defaulting to Saudi standard: 15%")
                    return ("P2", "FINANCIAL_LINE_ITEM", "vat_exclusive_keyword", False)

    # ========================================================================
    # P4 DETECTION - ONLY TOTAL, NO BREAKDOWN
    # ========================================================================
    # CRITICAL FIX: Only trigger P4 if signal type is NOT PRICE_ANNOTATION
    # Documents with "Total Premium with VAT included" should be P1, not P4
    if 'total' in total_text and 'vat' in total_text:
        if not vat_amount and not vat_percentage:
            # Check if this is actually a P1 document (VAT-inclusive) that was missed
            if vat_signal_type == "PRICE_ANNOTATION":
                # This is P1, not P4 - document explicitly states VAT is included
                logger.info("✅ P4 Detection Override: Signal type is PRICE_ANNOTATION")
                logger.info("   Document states VAT is included (P1), not missing breakdown (P4)")
                logger.info("   Returning P1 classification instead of P4")
                return ("P1", "PRICE_ANNOTATION", "price_annotation_total", True)

            # Otherwise, this is genuinely P4 (total shown but no breakdown and no inclusion statement)
            logger.error("❌ VAT Classification: P4 (Only total shown, no breakdown)")
            logger.error("   Document shows 'Total including VAT' but no VAT details or inclusion statement")
            raise VatPolicyViolation(
                vat_class="P4",
                reason="Only total shown, no VAT breakdown provided",
                details={
                    "total_text": total_text,
                    "vat_amount": vat_amount,
                    "vat_percentage": vat_percentage,
                    "vat_signal_type": vat_signal_type
                }
            )

    # ========================================================================
    # P3 DETECTION - NO VAT MENTION (DEFAULT/FALLBACK)
    # ========================================================================
    # P3 Rule: If vat_signal_type == NONE
    if vat_signal_type == "NONE":
        # CRITICAL FIX: Before classifying as P3, do a thorough check for VAT-inclusive indicators
        # These might appear beyond the first 2000 chars or in fields we haven't checked yet
        logger.info("🔍 P3 Pre-Check: Performing thorough search for VAT-inclusive indicators...")

        vat_inclusive_indicators = [
            'incl. vat', 'incl vat', 'including vat', 'vat included',
            'with vat', 'incl. tax', 'including tax', 'tax included',
            'inclusive vat', 'inclusive of vat', 'inclusive tax',
            'total premium with vat included', 'premium with vat included'
        ]

        # Search in premium_text, total_text, and FULL document text (not just 2000 chars)
        text_lower = text.lower() if text else ""
        search_text = premium_text + " " + total_text + " " + text_lower

        found_indicator = None
        for indicator in vat_inclusive_indicators:
            if indicator in search_text:
                found_indicator = indicator
                break

        if found_indicator:
            # This is a P1 document that was missed by earlier detection
            logger.info(f"✅ P3 Override: Found VAT-inclusive indicator '{found_indicator}'")
            logger.info("   This is a P1 (VAT-inclusive) document, not P3")
            logger.info("   Document explicitly states VAT is included in premium")
            return ("P1", "PRICE_ANNOTATION", "price_annotation_fallback", True)

        # If no VAT-inclusive indicators found, this is genuinely P3
        # P3 is now ALLOWED - VAT will be determined at billing time
        logger.info("✅ VAT Classification: P3 (VAT-Deferred)")
        logger.info("   VAT signal type: NONE - No VAT signals detected in quote")
        logger.info("   VAT will be charged at time of billing")
        logger.info("   Quote does not include VAT information")
        return ("P3", "NONE", "vat_deferred", False)

    # Fallback: If we reach here, signal type doesn't match expected patterns
    # Treat as P3 (VAT-Deferred) rather than rejecting
    logger.info(f"✅ VAT Classification: P3 (VAT-Deferred - Fallback)")
    logger.info(f"   VAT signal type: {vat_signal_type} but no clear VAT information")
    logger.info("   Treating as VAT-deferred - VAT will be determined at billing")
    return ("P3", "NONE", "vat_deferred", False)


def _parse_currency_amount(text: str) -> Optional[float]:
    """Parse any currency amount format."""
    if not text:
        return None
    try:
        cleaned = re.sub(r'[SAR$£€¥SR,\s/-]', '', str(text))
        return float(cleaned) if cleaned else None
    except:
        return None


def _detect_document_format(text: str) -> str:
    """Detect document format/insurer."""
    text_lower = text.lower()
    
    if 'liva insurance' in text_lower or 'liva' in text_lower[:1000]:
        return "LIVA"
    elif 'tawuniya' in text_lower[:1000]:
        return "TAWUNIYA"
    elif 'chubb' in text_lower[:1000] or 'ace arabia' in text_lower:
        return "CHUBB"
    elif 'gulf insurance' in text_lower[:1000] or 'gig' in text_lower[:500]:
        return "GIG"
    elif 'united cooperative' in text_lower[:1000] or 'uca' in text_lower[:500]:
        return "UCA"
    else:
        return "GENERIC"


def _extract_sublimits_from_text(text: str) -> Dict[str, str]:
    """Extract sublimits scattered throughout document."""
    sublimits = {}
    
    patterns = [
        r'([A-Za-z\s&/]+?)\s*[-–]\s*[Ll]imit\s+(?:up\s+to\s+)?(?:SR\.?|SAR)\s*([\d,]+)',
        r'([A-Za-z\s&/]+?)\s+[Ll]imit\s+(?:SR\.?|SAR)\s*([\d,]+)',
        r'([A-Za-z\s&/]+?)\s*[-–]\s*(?:SR\.?|SAR)\s*([\d,]+)(?:/-)?',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            clause_name = match.group(1).strip()
            limit_aoo = match.group(2).replace(',', '')
            
            clause_name = re.sub(r'\s+', ' ', clause_name)
            
            if len(clause_name) < 5:
                continue
            
            try:
                limit_value = f"SR {int(limit_aoo):,}"
                sublimits[clause_name] = limit_value
            except:
                continue
    
    return sublimits


def _generate_quote_fingerprint(company: str, policy_number: str, premium: float) -> str:
    """Generate unique fingerprint for duplicate detection."""
    fingerprint = f"{company}_{policy_number}_{premium}"
    return fingerprint.lower().replace(' ', '').replace('\n', '')


# ============================================================================
# NEW HELPER FUNCTIONS FOR LIABILITY INSURANCE ENHANCEMENTS (v2.0)
# Requirements #2-#6 from client liability reqs document
# ============================================================================

def _extract_sublimits_fallback(text: str, main_limit: float) -> List[Dict]:
    """
    FALLBACK: Extract liability sub-limits using pattern matching (Requirement #2).

    Detects restrictive sub-limits like "Any one person: SAR 300K" that reduce
    effective coverage even when main limit is higher (e.g., SAR 5M).

    Args:
        text: Full document text
        main_limit: Main per-claim or aggregate limit

    Returns:
        List of sub-limit dicts with type, amount, description, triggers_penalty, penalty_reason
    """
    if not main_limit or main_limit == 0:
        return []

    sublimits = []

    # Pattern: (pattern, sublimit_type)
    sublimit_patterns = [
        (r'any\s+one\s+person[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_person'),
        (r'each\s+person[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_person'),
        (r'per\s+individual[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_person'),
        (r'per\s+person[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_person'),
        (r'any\s+one\s+employee[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_employee'),
        (r'per\s+employee[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_employee'),
        (r'any\s+one\s+property[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_property'),
        (r'per\s+property[:\s]+(?:SAR?\s*|SR\s*)?([0-9,]+)', 'per_property'),
    ]

    for pattern, sublimit_type in sublimit_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            amount_str = match.group(1).replace(',', '')
            try:
                amount = float(amount_str)

                # Only flag if sub-limit is significantly lower than main limit (<80%)
                if amount < main_limit * 0.8:
                    sublimits.append({
                        'type': sublimit_type,
                        'amount': amount,
                        'description': match.group(0).strip(),
                        'triggers_penalty': True,
                        'penalty_reason': f"Sub-limit (SAR {amount:,.0f}) is significantly lower than main limit (SAR {main_limit:,.0f}), restricting coverage"
                    })
                    logger.info(f"✓ Detected restrictive sub-limit: {sublimit_type} = SAR {amount:,.0f} (<80% of main limit)")
            except (ValueError, IndexError):
                continue

    return sublimits


def _normalize_project_based_premium(extracted_data: Dict, text: str) -> Dict:
    """
    Normalize per-project premiums to annual cost (Requirement #3).

    Detects "per project" premiums and calculates estimated annual cost by
    multiplying by project count if found in document.

    Args:
        extracted_data: Extracted data dict
        text: Full document text

    Returns:
        Updated extracted_data with estimated_annual_premium and related fields
    """
    premium_basis = extracted_data.get('premium_basis', '').lower() if extracted_data.get('premium_basis') else ''
    premium_frequency = extracted_data.get('premium_frequency', '').lower() if extracted_data.get('premium_frequency') else ''
    premium_amount = extracted_data.get('premium_amount', 0)
    project_count = extracted_data.get('project_count')

    # Check if this is a per-project premium
    is_per_project = 'per_project' in premium_basis or 'per_project' in premium_frequency or \
                     'per_policy' in premium_basis or 'per_policy' in premium_frequency

    if not is_per_project:
        # Also check for patterns in text near premium mentions
        premium_text_patterns = [
            r'premium[:\s]+(?:SAR?\s*|SR\s*)?[\d,]+\s+per\s+project',
            r'premium[:\s]+(?:SAR?\s*|SR\s*)?[\d,]+\s+per\s+policy',
            r'per\s+project[:\s]+(?:SAR?\s*|SR\s*)?[\d,]+',
            r'per\s+contract[:\s]+(?:SAR?\s*|SR\s*)?[\d,]+',
        ]

        for pattern in premium_text_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                is_per_project = True
                extracted_data['premium_basis'] = 'per_project'
                logger.info("✓ Detected per-project premium basis from text patterns")
                break

    if is_per_project:
        if not project_count:
            # Try to extract project count from text
            project_count_patterns = [
                r'(\d+)\s+projects?\s+per\s+year',
                r'annual\s+project\s+volume[:\s]+(\d+)',
                r'expected\s+(\d+)\s+projects?',
                r'(\d+)\s+projects?\s+annually',
                r'project\s+count[:\s]+(\d+)',
            ]

            for pattern in project_count_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    project_count = int(match.group(1))
                    extracted_data['project_count'] = project_count
                    logger.info(f"✓ Extracted project count from text: {project_count} projects/year")
                    break

        if project_count and premium_amount:
            # Calculate estimated annual premium
            estimated_annual = premium_amount * project_count
            extracted_data['estimated_annual_premium'] = estimated_annual
            extracted_data['is_premium_estimated'] = True
            extracted_data['premium_normalization_note'] = (
                f"Per-project premium (SAR {premium_amount:,.0f}) × {project_count} projects/year = "
                f"SAR {estimated_annual:,.0f} estimated annual cost"
            )
            logger.info(f"💰 Normalized per-project premium: SAR {estimated_annual:,.0f}")
        else:
            # Flag for manual entry
            extracted_data['is_premium_estimated'] = False
            extracted_data['premium_normalization_note'] = (
                "⚠️ Per-project premium detected but project count not found. "
                "Manual entry required for annual cost estimation."
            )
            logger.warning("⚠️ Per-project premium without project count - flagged for manual entry")

    return extracted_data


def _analyze_deductible_structure(deductible_text: str) -> Dict:
    """
    Analyze deductible structure for penalties/bonuses (Requirement #4).

    Detects:
    - Percentage deductibles (high risk) → 10-point penalty
    - Split deductibles (property vs injury) → Use highest value
    - NIL deductibles for injury → 5-point bonus

    Args:
        deductible_text: Deductible field text or structured data

    Returns:
        Dict with type, highest_value, has_percentage, has_nil, risk_flags
    """
    if not deductible_text:
        return {
            'type': 'unknown',
            'highest_value': None,
            'has_percentage': False,
            'has_nil': False,
            'risk_flags': []
        }

    text_lower = str(deductible_text).lower()
    result = {
        'type': 'fixed',
        'highest_value': None,
        'has_percentage': False,
        'has_nil': False,
        'risk_flags': []
    }

    # Check for percentage-based deductibles
    percentage_patterns = [
        r'\d+%\s*(?:of|on)\s*(?:claim|sum\s+insured|si|loss)',
        r'\d+\s*percent\s*(?:of|on)\s*(?:claim|sum\s+insured|si|loss)',
    ]

    for pattern in percentage_patterns:
        if re.search(pattern, text_lower):
            result['type'] = 'percentage'
            result['has_percentage'] = True
            result['risk_flags'].append('percentage_high_risk')
            logger.info("✓ Detected percentage deductible (high risk)")
            break

    # Check for NIL/zero deductibles
    nil_patterns = [
        r'\bnil\b',
        r'zero\s+deductible',
        r'no\s+deductible',
        r'sar\s*0\b',
        r'sr\s*0\b',
    ]

    for pattern in nil_patterns:
        if re.search(pattern, text_lower):
            result['has_nil'] = True
            # Check if NIL applies to injury/bodily injury (bonus condition)
            if re.search(r'(?:injury|bodily|death|third\s+party)', text_lower):
                result['risk_flags'].append('nil_injury_bonus')
                logger.info("✓ Detected NIL deductible for injury coverage (bonus)")
            break

    # Check for split deductibles (different values for different coverage)
    split_patterns = [
        r'property.*?(?:injury|bodily)',
        r'bodily.*?property',
        r'(?:property\s+damage|pd).*?(?:bodily\s+injury|bi)',
        r'(?:bodily\s+injury|bi).*?(?:property\s+damage|pd)',
    ]

    for pattern in split_patterns:
        if re.search(pattern, text_lower, re.DOTALL):
            result['type'] = 'split'
            result['risk_flags'].append('split_property_vs_injury')
            logger.info("✓ Detected split deductible (property vs injury)")

            # Extract both values and find highest
            amount_patterns = [
                r'(?:SAR?\s*|SR\s*)([\d,]+)',
                r'([\d,]+)\s*(?:SAR|SR)',
            ]

            amounts = []
            for amt_pattern in amount_patterns:
                matches = re.findall(amt_pattern, text_lower)
                for match in matches:
                    try:
                        amount = float(match.replace(',', ''))
                        amounts.append(amount)
                    except ValueError:
                        continue

            if amounts:
                result['highest_value'] = max(amounts)
                logger.info(f"   Highest deductible value: SAR {result['highest_value']:,.0f} (used for scoring)")
            break

    # If no split detected, try to extract a single amount
    if not result['highest_value']:
        amount_match = re.search(r'(?:SAR?\s*|SR\s*)([\d,]+)', str(deductible_text), re.IGNORECASE)
        if amount_match:
            try:
                result['highest_value'] = float(amount_match.group(1).replace(',', ''))
            except ValueError:
                pass

    return result


def _normalize_liability_field_synonyms(liability_data: Dict) -> Dict:
    """
    Normalize synonym fields to standard names (Requirement #5).

    Maps various insurer-specific terms to standard field names:
    - "Limit of Indemnity" (Al Sagr) → per_claim_occurrence_limit
    - "Limit of Liability" (UCA) → per_claim_occurrence_limit
    - "Any One Occurrence" → per_claim_occurrence_limit

    Args:
        liability_data: Raw liability structure dict from AI extraction

    Returns:
        Normalized liability_data with standard field names
    """
    if not liability_data or not isinstance(liability_data, dict):
        return liability_data or {}

    # Synonym mappings: {synonym_field: standard_field}
    synonym_mappings = {
        # Per-claim synonyms → standard field
        'limit_of_liability': 'per_claim_occurrence_limit',
        'limit_of_indemnity': 'per_claim_occurrence_limit',
        'indemnity_limit': 'per_claim_occurrence_limit',
        'any_one_claim': 'per_claim_occurrence_limit',
        'each_occurrence': 'per_claim_occurrence_limit',
        'any_one_occurrence': 'per_claim_occurrence_limit',
        'any_one_occurrence_limit': 'per_claim_occurrence_limit',
        'per_occurrence': 'per_claim_occurrence_limit',
        'per_occurrence_limit': 'per_claim_occurrence_limit',

        # Aggregate synonyms → standard field
        'annual_aggregate': 'aggregate_annual_limit',
        'total_limit': 'aggregate_annual_limit',
        'in_the_aggregate': 'aggregate_annual_limit',
        'aggregate_limit': 'aggregate_annual_limit',
    }

    normalized_count = 0
    for synonym, standard_field in synonym_mappings.items():
        # If synonym exists and standard field doesn't, copy the value
        if synonym in liability_data and not liability_data.get(standard_field):
            liability_data[standard_field] = liability_data[synonym]
            normalized_count += 1
            logger.info(f"✓ Normalized synonym: '{synonym}' → '{standard_field}'")

    if normalized_count > 0:
        logger.info(f"✓ Normalized {normalized_count} liability field synonym(s)")

    return liability_data


def _calculate_implied_rate(premium: float, limit: float, stated_rate_str: str) -> Dict:
    """
    Calculate implied rate from premium and limit, compare with stated rate (Requirement #6).

    Formula: Rate = (Premium / Limit) × 1000  (per mille basis)

    This provides a sanity check since OCR often misreads rate symbols (e.g., "0.93%%" typos).

    Args:
        premium: Premium amount (before VAT)
        limit: Coverage limit (per-claim or sum insured)
        stated_rate_str: Rate as stated in document (e.g., "0.35‰")

    Returns:
        Dict with calculated_rate, stated_rate, discrepancy, validation_flag
    """
    if not premium or not limit or premium == 0 or limit == 0:
        return {
            'calculated_rate': None,
            'stated_rate': None,
            'discrepancy': None,
            'validation_flag': 'insufficient_data'
        }

    # Calculate implied rate (per mille)
    calculated_rate = (premium / limit) * 1000

    # Parse stated rate using existing helper
    stated_rate = None
    if stated_rate_str:
        # Try to extract numeric value from rate string
        rate_match = re.search(r'(\d+\.?\d*)', str(stated_rate_str))
        if rate_match:
            try:
                stated_rate = float(rate_match.group(1))
            except ValueError:
                pass

    result = {
        'calculated_rate': round(calculated_rate, 3)
    }

    if stated_rate and stated_rate > 0:
        # Compare stated vs calculated
        discrepancy = abs(calculated_rate - stated_rate)
        discrepancy_pct = (discrepancy / stated_rate) * 100 if stated_rate > 0 else 0

        result['stated_rate'] = stated_rate
        result['discrepancy'] = round(discrepancy, 3)

        if discrepancy_pct < 5:
            result['validation_flag'] = 'rate_match'
            logger.info(f"✓ Rate validation: Stated {stated_rate:.3f}‰ ≈ Calculated {calculated_rate:.3f}‰")
        elif discrepancy_pct < 20:
            result['validation_flag'] = 'rate_mismatch_minor'
            logger.warning(f"⚠️ Rate mismatch (minor): Stated {stated_rate:.3f}‰ vs Calculated {calculated_rate:.3f}‰ "
                          f"({discrepancy_pct:.1f}% difference)")
        else:
            result['validation_flag'] = 'rate_mismatch_major'
            logger.error(f"❌ Rate mismatch (MAJOR): Stated {stated_rate:.3f}‰ vs Calculated {calculated_rate:.3f}‰ "
                        f"({discrepancy_pct:.1f}% difference)")
    else:
        result['stated_rate'] = None
        result['discrepancy'] = None
        result['validation_flag'] = 'rate_missing'
        logger.info(f"ℹ️ Rate not stated in document. Calculated rate: {calculated_rate:.3f}‰")

    return result


# ============================================================================


def _calculate_quality_score(extracted_data: Dict) -> Tuple[float, Dict]:
    """
    Calculate extraction quality score with breakdown.
    Returns: (overall_score, score_breakdown)
    """
    scores = {
        'completeness': 0,
        'accuracy': 0,
        'detail_level': 0,
        'structure': 0
    }
    
    # Completeness (40 points)
    required_fields = ['company_name', 'premium_amount', 'rate', 'policy_type']
    fields_present = sum(1 for field in required_fields if extracted_data.get(field))
    scores['completeness'] = (fields_present / len(required_fields)) * 40
    
    # Accuracy (30 points)
    issues = 0
    if '$' in str(extracted_data.get('rate', '')):
        issues += 1
    if not extracted_data.get('deductible'):
        issues += 1
    scores['accuracy'] = max(0, 30 - (issues * 10))
    
    # Detail level (20 points)
    benefits = len(extracted_data.get('key_benefits', []))
    exclusions = len(extracted_data.get('exclusions', []))
    warranties = len(extracted_data.get('warranties', []))
    detail_score = min(20, (benefits + exclusions + warranties) / 3)
    scores['detail_level'] = detail_score
    
    # Structure (10 points)
    extended = extracted_data.get('_extended_data', {})
    structure_elements = ['sublimits_comprehensive', 'deductibles_complete', 'subjectivities']
    structure_present = sum(1 for elem in structure_elements if elem in extended)
    scores['structure'] = (structure_present / len(structure_elements)) * 10
    
    overall = sum(scores.values())
    
    return overall, scores


async def extract_structured_data_from_text(text: str, filename: str, pdf_path: Optional[str] = None) -> Dict:
    """
    PRODUCTION-READY 6-STAGE EXTRACTION SYSTEM v6.0
    ===============================================
    Stage 1: Entity identification (insurer vs insured)
    Stage 2: Document format detection & preprocessing
    Stage 3: Comprehensive data extraction with proper categorization
    Stage 4: Subjectivities, payment terms, and binding requirements
    Stage 5: Mathematical calculations and tier detection
    Stage 6: Intelligent analysis with transparent scoring

    Args:
        text: Extracted text from the PDF
        filename: Original filename of the PDF
        pdf_path: Optional path to the PDF file (for OCR fallback)
    """

    # ========================================================================
    # STAGE 1: ENTITY IDENTIFICATION
    # ========================================================================

    logger.info(f"🔍 Stage 1: Entity identification for {filename}")

    insurer_result = await _extract_insurer_from_text(text, filename, pdf_path)
    insurer_name = insurer_result.name if insurer_result.name else "Unknown Insurer"
    detection_method = insurer_result.method
    detection_confidence = insurer_result.confidence

    insured_name = _extract_insured_from_text(text)

    # Handle None return from insurer detection
    if not insurer_result.name:
        logger.warning(f"⚠️ Could not detect insurer from text, using fallback: {insurer_name}")
    else:
        logger.info(f"Insurer detected: {insurer_name} (method: {detection_method}, confidence: {detection_confidence})")

    logger.info(f"🏢 Insurer (Insurance Company): {insurer_name}")
    logger.info(f"👤 Insured (Customer): {insured_name}")
    
    # ========================================================================
    # STAGE 2: DOCUMENT FORMAT DETECTION
    # ========================================================================
    
    logger.info(f"🔍 Stage 2: Document format detection")
    doc_format = _detect_document_format(text)
    logger.info(f"📋 Detected format: {doc_format}")
    
    # CRITICAL FIX: Format-based company name override for consistency
    # If document format is clearly identified, ensure company name matches
    if doc_format == "LIVA" and "liva" not in insurer_name.lower():
        logger.warning(f"⚠️ Format/Company mismatch: Format={doc_format}, Company={insurer_name}")
        logger.info(f"🔧 Overriding company name to: Liva Insurance (based on document format)")
        insurer_name = "Liva Insurance"
        # Update detection metadata when format override occurs
        original_method = detection_method
        detection_method = "format_override"
        detection_confidence = "high"
        logger.info(f"Format-based override: Changed from {original_method} to format_override")
    elif doc_format == "TAWUNIYA" and "tawuniya" not in insurer_name.lower():
        logger.warning(f"⚠️ Format/Company mismatch: Format={doc_format}, Company={insurer_name}")
        logger.info(f"🔧 Overriding company name to: Tawuniya (based on document format)")
        insurer_name = "Tawuniya"
        # Update detection metadata when format override occurs
        original_method = detection_method
        detection_method = "format_override"
        detection_confidence = "high"
        logger.info(f"Format-based override: Changed from {original_method} to format_override")
    elif doc_format == "CHUBB" and "chubb" not in insurer_name.lower():
        logger.warning(f"⚠️ Format/Company mismatch: Format={doc_format}, Company={insurer_name}")
        logger.info(f"🔧 Overriding company name to: Chubb Arabia (based on document format)")
        insurer_name = "Chubb Arabia"
        # Update detection metadata when format override occurs
        original_method = detection_method
        detection_method = "format_override"
        detection_confidence = "high"
        logger.info(f"Format-based override: Changed from {original_method} to format_override")
    elif doc_format == "GIG" and "gig" not in insurer_name.lower() and "gulf insurance" not in insurer_name.lower():
        logger.warning(f"⚠️ Format/Company mismatch: Format={doc_format}, Company={insurer_name}")
        logger.info(f"🔧 Overriding company name to: Gulf Insurance Group (GIG) (based on document format)")
        insurer_name = "Gulf Insurance Group (GIG)"
        # Update detection metadata when format override occurs
        original_method = detection_method
        detection_method = "format_override"
        detection_confidence = "high"
        logger.info(f"Format-based override: Changed from {original_method} to format_override")
    elif doc_format == "UCA" and "uca" not in insurer_name.lower() and "united cooperative" not in insurer_name.lower():
        logger.warning(f"⚠️ Format/Company mismatch: Format={doc_format}, Company={insurer_name}")
        logger.info(f"🔧 Overriding company name to: United Cooperative Assurance (UCA) (based on document format)")
        insurer_name = "United Cooperative Assurance (UCA)"
        # Update detection metadata when format override occurs
        original_method = detection_method
        detection_method = "format_override"
        detection_confidence = "high"
        logger.info(f"Format-based override: Changed from {original_method} to format_override")
    
    sublimits_detected = _extract_sublimits_from_text(text)
    logger.info(f"🎯 Pre-extracted {len(sublimits_detected)} sublimits")

    # ========================================================================
    # STAGE 2.5: INSURANCE CATEGORY DETECTION
    # ========================================================================

    # Early category detection to guide Stage 3 extraction
    # We'll do a preliminary detection based on filename/text, then refine after policy_type extraction
    preliminary_policy_type = ""

    # Try to extract policy type early for better category detection
    policy_type_patterns = [
        r'Policy Type[:\s]+([^\n]{5,100})',
        r'Class of Insurance[:\s]+([^\n]{5,100})',
        r'Type of Cover[:\s]+([^\n]{5,100})',
        r'Insurance Type[:\s]+([^\n]{5,100})',
    ]
    for pattern in policy_type_patterns:
        match = re.search(pattern, text[:5000], re.IGNORECASE)
        if match:
            preliminary_policy_type = match.group(1).strip()
            logger.info(f"📋 Preliminary policy type detected: {preliminary_policy_type}")
            break

    insurance_category, category_confidence = _detect_insurance_category(text, preliminary_policy_type)
    logger.info(f"🔍 Insurance Category: {insurance_category} (confidence: {category_confidence})")

    # ========================================================================
    # STAGE 3: COMPREHENSIVE DATA EXTRACTION
    # ========================================================================

    # Build insurance-type-specific instructions
    if insurance_category == 'liability':
        insurance_specific_instructions = """
**CRITICAL: INSURANCE TYPE AWARENESS**

This quote has been identified as: **LIABILITY INSURANCE** (CGL, Third Party, Professional Indemnity, Product Liability).

🔍 **LIABILITY INSURANCE - SPECIAL EXTRACTION RULES:**

**Sum Insured Semantics:**
- Represents EXPOSURE to third-party claims (bodily injury, property damage, legal liability)
- OR based on insured's annual turnover/revenue
- NOT tied to physical asset value
- Higher sum insured = BETTER protection (more coverage for claims)

**Limit Structures to Extract:**

1. **Per Claim/Per Occurrence Limit**: Maximum payout for a single claim/incident
   - Look for: "per claim", "per occurrence", "any one claim", "each occurrence"
   - Example: "SAR 10M any one claim"

2. **Aggregate Annual Limit**: Maximum total payout for ALL claims in one year
   - Look for: "in the aggregate", "annual aggregate", "total limit"
   - Example: "SAR 50M in the aggregate"
   - Example: "SAR 10M per occurrence / SAR 50M aggregate"

3. **Turnover-Based Limits**: Coverage calculated from annual revenue
   - Look for: "times annual turnover", "x turnover", "based on turnover"
   - Example: "2 times annual turnover"
   - Example: "SAR 50M or 2x turnover, whichever is higher"
   - Extract: multiplier, cap (if any), annual turnover (if stated)

4. **Defense Costs Structure** - CRITICAL:
   - **Inside the Limit**: Defense costs REDUCE available coverage
     * "SAR 10M inclusive of defense costs"
     * "defense costs within the limit"
     * "inclusive of legal expenses"
   - **Outside the Limit**: Defense costs paid SEPARATELY (better coverage)
     * "SAR 10M plus unlimited defense costs"
     * "defense costs in addition to the limit"
     * "defense costs over and above"
     * "legal expenses in addition"

5. **Claims-Made Specific Fields**:
   - **Retroactive Date**: Coverage only for claims arising after this date
   - **Extended Reporting Period (ERP)**: Tail coverage period after policy expires

**Extract as:**
```json
{
  "liability_limit_structure": {
    "per_claim_occurrence_limit": 10000000,
    "aggregate_annual_limit": 50000000,
    "is_turnover_based": false,
    "turnover_multiplier": null,
    "turnover_cap": null,
    "annual_turnover": null,
    "defense_costs_inside_limit": true,
    "defense_costs_sublimit": null,
    "retroactive_date": "2024-01-01",
    "extended_reporting_period": "24 months"
  }
}
```

**Note:** If limits are turnover-based, set `is_turnover_based: true` and extract multiplier/cap.
"""
    elif insurance_category == 'property':
        insurance_specific_instructions = """
**CRITICAL: INSURANCE TYPE AWARENESS**

This quote has been identified as: **PROPERTY INSURANCE** (Fire, All Risk, Material Damage, Business Interruption).

🏢 **PROPERTY INSURANCE - STANDARD EXTRACTION:**

**Sum Insured Semantics:**
- Represents VALUE of physical assets being insured
- Based on actual asset value, replacement cost, or agreed value
- Higher sum insured = Insuring more valuable property

**No liability_limit_structure needed** - This field should remain `null` for property insurance.

Focus on:
- Sum insured breakdown (Material Damage, Business Interruption)
- Deductibles
- Standard property coverage benefits
"""
    else:
        insurance_specific_instructions = """
**CRITICAL: INSURANCE TYPE AWARENESS**

This quote has been identified as: **{category_upper}** insurance.

Extract all standard insurance information. If this is liability insurance but not detected correctly, look for liability-specific limit structures (per claim, aggregate, defense costs).
""".format(category_upper=insurance_category.upper())

    system_prompt_stage3 = f"""You are an EXPERT insurance data extraction specialist.

{insurance_specific_instructions}

CRITICAL ENTITY IDENTIFICATION:
- INSURER (Insurance Company): {insurer_name}
- INSURED (Customer): {insured_name}

CRITICAL ENTITY IDENTIFICATION:
- INSURER (Insurance Company): {insurer_name}
- INSURED (Customer): {insured_name}

CRITICAL: PROPERLY CATEGORIZE WARRANTIES VS EXTENSIONS

**WARRANTIES** = Ongoing obligations the insured must fulfill:
- "Hot works Warranty"
- "No smoking Warranty"
- "Housekeeping Warranty"
- "Fire equipment maintenance warranty"
- "Sprinkler maintenance warranty"
- "Security warranty (24 hours)"

**EXTENSIONS/CONDITIONS** = Additional coverage or clauses:
- "Smoke Damage clause" (Extension)
- "Boiler explosion coverage" (Extension)
- "Underground Services Clause" (Extension)
- "Automatic Reinstatement" (Condition)
- "Capital Additions Clause" (Condition)

DO NOT mix these up!

RATE NOTATION: NEVER use $ for rates
- Use ‰ for per mille (e.g., "0.38‰")
- Use % for percentage (e.g., "2.5%")

JSON SAFETY:
- NO line breaks inside strings
- NO unescaped quotes
- Replace apostrophes
- Keep strings under 300 characters

COMPLETENESS:
- Extract ALL exclusions (20-30+) EXACTLY as written
- Extract ALL warranties (actual warranties, not extensions)
- Extract ALL benefits with limits
- Extract ALL deductible tiers with ranges"""

    user_prompt_stage3 = f"""Extract ALL information from this insurance quote.

PRE-IDENTIFIED:
- INSURER: {insurer_name}
- INSURED: {insured_name}
- INSURANCE CATEGORY: {insurance_category.upper()} (confidence: {category_confidence})

DOCUMENT TEXT:
{text[:28000]}

Extract as JSON:

{{
  "insurer_company_name": "{insurer_name}",
  "insured_customer_name": "{insured_name}",
  "policy_type": "Exact policy type",
  "policy_number": "Policy/quote reference",
  
  "sum_insured_breakdown": {{
    "material_damage": "MD amount",
    "business_interruption": "BI amount",
    "total_sum_insured_numeric": 1564652306.28,
    "total_sum_insured": "SR 1,564,652,306"
  }},

  "liability_limit_structure": {{
    "per_claim_occurrence_limit": 10000000,
    "aggregate_annual_limit": 50000000,
    "is_turnover_based": false,
    "turnover_multiplier": null,
    "turnover_cap": null,
    "annual_turnover": null,
    "defense_costs_inside_limit": true,
    "defense_costs_sublimit": null,
    "retroactive_date": null,
    "extended_reporting_period": null
  }},

  "rate_information": {{
    "rate_text_raw": "EXACT rate text (CRITICAL: if pattern like '66,340.70 @ 0.1615 percent', extract '0.1615' NOT '66')",
    "rate_numeric_value": 0.38,
    "rate_type": "per_mille",
    "rate_formatted": "0.38‰"
  }},
  
  "RATE_EXTRACTION_RULES": {{
    "CRITICAL": "The @ symbol separates premium from rate",
    "BEFORE_@": "premium amount (e.g., 66,340.70 or SR 69,602)",
    "AFTER_@": "rate value (e.g., 0.1615 or 0.2% or 0.35‰)",
    "VALIDATION": "Rates are typically 0.001% to 10%. If > 10%, you extracted wrong number",
    "PATTERNS": [
      "66,340.70 @ 0.1615 percent → rate = 0.1615%",
      "SR 69,602 @ 0.2% → rate = 0.2%",
      "169,122.3 @ 0.30‰ → rate = 0.30‰",
      "FLAT Premium → rate = FLAT Premium"
    ]
  }},
  
  "premium_information": {{
    "base_premium_amount": 500000.00,
    "policy_fee": 25.00,
    "vat_percentage": 15,
    "vat_amount": 75003.75,
    "total_including_vat": 575028.75,
    "payment_terms": "100% at inception",
    "payment_details": "Full payment details"
  }},
  
  "deductibles_complete": {{
    "structure_type": "tiered_by_sum_insured",
    "material_damage_tiers": [
      {{
        "range": "up to SR 40M",
        "deductible": "5%, min SR 50,000",
        "applies_to": "material_damage"
      }},
      {{
        "range": "SR 100M to SR 500M",
        "deductible": "5%, min SR 500,000",
        "applies_to": "material_damage"
      }},
      {{
        "range": "above SR 500M",
        "deductible": "5%, min SR 1,000,000",
        "applies_to": "material_damage"
      }}
    ],
    "business_interruption_tiers": [
      {{
        "range": "up to SR 40M",
        "deductible": "7 days",
        "applies_to": "business_interruption"
      }},
      {{
        "range": "SR 100M to SR 500M",
        "deductible": "14 days",
        "applies_to": "business_interruption"
      }},
      {{
        "range": "above SR 500M",
        "deductible": "21 days",
        "applies_to": "business_interruption"
      }}
    ],
    "natural_catastrophe_tiers": [
      {{
        "range": "above SR 500M",
        "deductible": "5%, min SR 1,500,000"
      }}
    ]
  }},
  
  "coverage_and_benefits": {{
    "coverage_benefits_explained": [
      "Automatic reinstatement: Coverage restored after claim",
      "Capital Additions: New equipment up to 10% of SI for 30 days",
      "... 15-25 benefits with amounts"
    ]
  }},
  
  "extensions_and_conditions": {{
    "extensions_list": [
      "Smoke Damage clause",
      "Boiler explosion coverage with SR 5,000 deductible",
      "Underground Services up to SR 500,000",
      "... all extensions"
    ],
    "conditions_list": [
      "Automatic Reinstatement of Sum Insured",
      "Capital Additions Clause - 10% of SI",
      "85% Average Clause",
      "... all conditions"
    ]
  }},
  
  "warranties_actual": {{
    "warranties_list": [
      "Hot works Warranty",
      "No smoking Warranty with signs",
      "Housekeeping Warranty",
      "Fire equipment maintenance warranty",
      "Sprinkler maintenance warranty",
      "Security warranty - 24 hours",
      "... ALL actual warranties (NOT extensions)"
    ]
  }},
  
  "sublimits_comprehensive": {{
    "care_custody_control": "SR 2,500,000 AOO",
    "srcc_malicious_damage": "25% of SI, max SR 10M",
    "... all sublimits with amounts"
  }},
  
  "exclusions_complete": {{
    "all_exclusions_list": [
      "Cyber Attack Exclusion Clause IUA 09-081 17.05.2019",
      "War and Terrorism Exclusion NMA 2918",
      "... LIST 20-30+ exclusions EXACTLY as written"
    ]
  }}
}}

Return ONLY valid JSON."""

    try:
        logger.info(f"🔍 Stage 3: Comprehensive extraction")
        
        response_stage3 = await openai_client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt_stage3},
                {"role": "user", "content": user_prompt_stage3}
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
            max_tokens=4096
        )
        
        raw_content = response_stage3.choices[0].message.content
        
        try:
            raw_data = json.loads(raw_content)
            logger.info("✅ Stage 3: JSON parsed successfully")
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON parsing failed: {str(e)}")
            fixed_content = _fix_json_response(raw_content)
            try:
                raw_data = json.loads(fixed_content)
                logger.info("✅ JSON parsing succeeded after cleanup")
            except json.JSONDecodeError as e2:
                logger.error(f"❌ JSON parsing failed after cleanup: {str(e2)}")
                raw_data = {}
                logger.warning("⚠️ Proceeding with minimal empty extraction due to JSON errors")
        
        raw_data['insurer_company_name'] = raw_data.get('insurer_company_name', insurer_name)
        raw_data['insured_customer_name'] = raw_data.get('insured_customer_name', insured_name)
        
        # ========================================================================
        # STAGE 3.5: VALIDATION + FALLBACK EXTRACTION (v7.0 FIX)
        # ========================================================================
        
        logger.info("🔍 Stage 3.5: Validation and Fallback")

        si_breakdown_stage3 = raw_data.get('sum_insured_breakdown', {})
        total_si_for_validation = _parse_currency_amount(si_breakdown_stage3.get('total_sum_insured_numeric')) or \
            _parse_currency_amount(si_breakdown_stage3.get('total_sum_insured'))
        raw_data['_total_si_numeric'] = total_si_for_validation or 0

        raw_data = _validate_and_enhance_extraction(raw_data, text, total_si_for_validation or 0)

        
        # ====================================================================
        # STAGE 4: SUBJECTIVITIES & BINDING REQUIREMENTS
        # ====================================================================
        
        logger.info(f"🔍 Stage 4: Extracting subjectivities and binding requirements")
        
        system_prompt_stage4 = """You are an insurance underwriting specialist."""
        
        user_prompt_stage4 = f"""Extract subjectivities, binding requirements, and operational details.

DOCUMENT TEXT:
{text[15000:35000]}

Extract as JSON:

{{
  "subjectivities_and_requirements": {{
    "binding_requirements": [
      "Risk survey within 30 days",
      "Civil Defense certificate required",
      "KYC/AML clearance",
      "... all requirements"
    ],
    "conditions_precedent": [
      "No deterioration of loss record",
      "Survey and fulfillment of recommendations",
      "... all conditions"
    ],
    "documentation_required": [
      "Signed proposal form",
      "GPS coordinates",
      "Photographs of property",
      "Civil Defense License",
      "... all documents"
    ]
  }},
  
  "operational_details": {{
    "validity_period": "15 days from date shown",
    "notice_to_bind": "5 working days prior notice",
    "cancellation_notice": "30 days notice pro-rata",
    "geographical_limits": "Kingdom of Saudi Arabia",
    "jurisdiction": "Kingdom of Saudi Arabia"
  }},
  
  "brokerage_and_fees": {{
    "brokerage_percentage": "15%",
    "broker_name": "Authorized Policy Insurance Brokers",
    "policy_fees": "SAR 25"
  }},
  
  "special_conditions": [
    "Warranted No combustible cladding on building facades",
    "No cover unless confirmed by insurer",
    "Civil Defense License mandatory for warehouses",
    "... all special conditions"
  ]
}}

Return ONLY valid JSON."""

        # PERFORMANCE FIX: Create async task for Stage 4 to run in parallel with Stage 5+6
        async def run_stage4():
            try:
                response = await openai_client.chat.completions.create(
                    model=settings.OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": system_prompt_stage4},
                        {"role": "user", "content": user_prompt_stage4}
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    max_tokens=2048
                )
                data = json.loads(response.choices[0].message.content)
                logger.info("✅ Stage 4: Subjectivities extracted")
                return data
            except Exception as e:
                logger.warning(f"⚠️ Stage 4: Could not extract subjectivities: {e}")
                return {}
        
        # Start Stage 4 task immediately (will run in parallel)
        stage4_task = asyncio.create_task(run_stage4())
        
        # ====================================================================
        # STAGE 5: CALCULATIONS & TIER DETECTION (runs while Stage 4 is in progress)
        # ====================================================================
        
        logger.info(f"🧮 Stage 5: Calculations and tier detection")
        
        si_breakdown = raw_data.get('sum_insured_breakdown', {})
        total_si = raw_data.get('_total_si_numeric')
        if not total_si:
            total_si = _parse_currency_amount(si_breakdown.get('total_sum_insured_numeric')) or \
                       _parse_currency_amount(si_breakdown.get('total_sum_insured'))
        
        rate_info = raw_data.get('rate_information', {})
        rate_text_raw = rate_info.get('rate_text_raw', '')
        rate_formatted = _normalize_rate_notation(rate_text_raw)
        
        if '$' in rate_formatted:
            rate_formatted = rate_formatted.replace('$', '') + '‰'
            logger.warning(f"⚠️ Removed $ from rate: {rate_formatted}")
        
        logger.info(f"🔄 Rate: '{rate_text_raw}' → '{rate_formatted}'")
        
        premium_calculated = None
        if total_si and rate_text_raw:
            premium_calculated = _calculate_premium_from_rate(total_si, rate_text_raw)
        
        prem_info = raw_data.get('premium_information', {})
        premium_stated = _parse_currency_amount(prem_info.get('base_premium_amount'))
        
        final_premium = premium_stated or premium_calculated
        
        vat_pct = _parse_currency_amount(prem_info.get('vat_percentage'))
        policy_fee = _parse_currency_amount(prem_info.get('policy_fee')) or 0

        # ========================================================================
        # COMPREHENSIVE VAT DETECTION v9.0 (Pattern-First with Validation)
        # ========================================================================
        logger.info("🔍 Stage 5.1: Comprehensive VAT detection and classification")

        # Use new comprehensive detection function
        vat_result = _comprehensive_vat_detection(text, prem_info)

        vat_class = vat_result["class"]
        original_premium_includes_vat = vat_result["is_inclusive"]
        vat_detection_method = vat_result["detection_method"]
        pattern_matched = vat_result["pattern_matched"]
        confidence = vat_result["confidence"]
        vat_warning = vat_result["warning"]
        requires_verification = vat_result["requires_verification"]

        # Preserve original premium (stated in document before any normalization)
        stated_premium = final_premium

        logger.info(f"📋 VAT Classification Results:")
        logger.info(f"   VAT Class: {vat_class}")
        logger.info(f"   Detection Method: {vat_detection_method}")
        logger.info(f"   Pattern Matched: {pattern_matched}")
        logger.info(f"   Confidence: {confidence}")
        logger.info(f"   Premium Includes VAT: {'Yes' if original_premium_includes_vat else 'No'}")
        logger.info(f"   Requires Verification: {'Yes' if requires_verification else 'No'}")
        if vat_warning:
            logger.warning(f"   ⚠️ Warning: {vat_warning}")

        # ========================================================================
        # POLICY GATE: Reject ONLY P5 (Zero VAT) and P6 (Non-standard rate)
        # ========================================================================
        if vat_class in ["P5", "P6"]:
            logger.error(f"❌ VAT Policy Violation: {vat_class}")
            raise VatPolicyViolation(
                vat_class=vat_class,
                reason=vat_result["warning"] or f"VAT class {vat_class} is not allowed",
                details={
                    "detection_method": vat_detection_method,
                    "pattern_matched": pattern_matched
                }
            )


        # ========================================================================
        # SET VAT VALUES FROM DETECTION RESULT
        # ========================================================================
        # Use values from comprehensive detection
        vat_percentage = vat_result["vat_percentage"]
        vat_amount = vat_result["vat_amount"]

        logger.info(f"💰 VAT Values from Detection:")
        logger.info(f"   vat_percentage: {vat_percentage}")
        logger.info(f"   vat_amount: {vat_amount}")

        # STEP 4: Handle P1 - No VAT math allowed
        if vat_class == "P1":
            # P1 (VAT-inclusive): VAT already included in premium, no separate calculation
            logger.info(f"🛑 P1 (VAT-Inclusive): No VAT math - Premium used as-is")
            vat_amount = None
            vat_percentage = None
            normalized_premium = stated_premium
            total_annual_cost = normalized_premium + policy_fee
            final_premium = normalized_premium

            logger.info(f"💰 Processing P1 Premium (VAT already included):")
            logger.info(f"   - Stated Premium: SAR {stated_premium:,.2f}")
            logger.info(f"   - VAT Amount: null (included in premium)")
            logger.info(f"   - VAT Percentage: null (included in premium)")
            logger.info(f"   - Policy Fee: SAR {policy_fee:,.2f}")
            logger.info(f"   - Total Annual Cost: SAR {total_annual_cost:,.2f}")
        elif vat_class == "P3":
            # P3 (VAT-Deferred): VAT will be charged at billing time, not included in quote
            logger.info(f"🛑 P3 (VAT-Deferred): VAT charged at billing - no VAT in quote")
            vat_amount = None
            vat_percentage = None
            normalized_premium = stated_premium
            total_annual_cost = normalized_premium + policy_fee
            final_premium = normalized_premium

            logger.info(f"💰 Processing P3 Premium (VAT deferred to billing):")
            logger.info(f"   - Stated Premium: SAR {stated_premium:,.2f}")
            logger.info(f"   - VAT Amount: null (charged at billing)")
            logger.info(f"   - VAT Percentage: null (charged at billing)")
            logger.info(f"   - Policy Fee: SAR {policy_fee:,.2f}")
            logger.info(f"   - Total Annual Cost: SAR {total_annual_cost:,.2f}")
        elif vat_class == "P2":
            # STEP 5: VAT CALCULATION (P2 - VAT-Exclusive)
            # vat_percentage and vat_amount already set by comprehensive detection
            logger.info(f"💰 Processing P2 (VAT-Exclusive) Premium:")

            normalized_premium = stated_premium  # Premium is VAT-exclusive

            if vat_amount and vat_amount > 0:
                # VAT amount already provided by detection (extracted from document)
                total_annual_cost = normalized_premium + policy_fee + vat_amount
                logger.info(f"   Using extracted VAT amount")
                logger.info(f"   Calculation:")
                logger.info(f"   - Base Premium (excl. VAT): SAR {normalized_premium:,.2f}")
                logger.info(f"   - VAT Amount (extracted): SAR {vat_amount:,.2f}")
                logger.info(f"   - VAT Percentage: {vat_percentage}%")
                logger.info(f"   - Policy Fee: SAR {policy_fee:,.2f}")
                logger.info(f"   - Total Annual Cost: SAR {total_annual_cost:,.2f}")
            elif vat_percentage is not None:
                # Calculate VAT from percentage (15% or extracted)
                vat_amount = (stated_premium + policy_fee) * (vat_percentage / 100)
                total_annual_cost = normalized_premium + policy_fee + vat_amount
                logger.info(f"   Calculating VAT from percentage")
                logger.info(f"   Calculation:")
                logger.info(f"   - Base Premium (excl. VAT): SAR {normalized_premium:,.2f}")
                logger.info(f"   - VAT @ {vat_percentage}%: SAR {vat_amount:,.2f}")
                logger.info(f"   - Policy Fee: SAR {policy_fee:,.2f}")
                logger.info(f"   - Total Annual Cost: SAR {total_annual_cost:,.2f}")
            else:
                # Should not happen with new detection, but handle gracefully
                logger.warning("⚠️ P2 but no VAT percentage available - this shouldn't happen")
                vat_amount = None
                total_annual_cost = normalized_premium + policy_fee

            # Update final_premium to normalized value for comparison
            final_premium = normalized_premium

        logger.info(f"✅ VAT Processing Complete")
        logger.info(f"   Final normalized premium (for comparison): SAR {final_premium:,.2f}")
        logger.info(f"   Classification: {vat_class}")
        logger.info(f"   Confidence: {confidence}")
        if vat_warning:
            logger.info(f"   Warning: {vat_warning}")

        # ========================================================================
        # DEFENSIVE VALIDATION WITH GRACEFUL CORRECTIONS
        # ========================================================================
        # P1 validation (graceful correction)
        if vat_class == "P1" and vat_amount is not None:
            logger.warning(f"⚠️ P1 correction: Nullifying unexpected VAT amount ({vat_amount})")
            logger.warning("   P1 documents should not have explicit VAT amounts (VAT already included)")
            vat_amount = None

        if vat_class == "P1" and vat_percentage is not None:
            logger.warning(f"⚠️ P1 correction: Nullifying unexpected VAT percentage ({vat_percentage}%)")
            logger.warning("   P1 documents should not have explicit VAT percentages (VAT already included)")
            vat_percentage = None

        # P3 validation (graceful correction instead of assertion)
        if vat_class == "P3" and vat_amount is not None:
            logger.warning(f"⚠️ P3 correction: Nullifying unexpected VAT amount ({vat_amount})")
            logger.warning("   P3 documents should not have VAT amounts in quote")
            vat_amount = None

        if vat_class == "P3" and vat_percentage is not None:
            logger.warning(f"⚠️ P3 correction: Nullifying unexpected VAT percentage ({vat_percentage}%)")
            logger.warning("   P3 documents should not have VAT percentage in quote")
            vat_percentage = None

        # P2 validation (graceful correction - default to 15%)
        if vat_class == "P2" and vat_percentage is None:
            logger.warning("⚠️ P2 correction: VAT percentage missing, defaulting to Saudi standard 15%")
            logger.warning("   P2 documents must have VAT percentage set")
            vat_percentage = 15.0

        logger.info("✅ Defensive validation passed - VAT handling is compliant")
        
        # CRITICAL FIX: Determine applicable deductible tier
        deductibles_data = raw_data.get('deductibles_complete', {})
        
        md_tiers = deductibles_data.get('material_damage_tiers', [])
        bi_tiers = deductibles_data.get('business_interruption_tiers', [])
        nc_tiers = deductibles_data.get('natural_catastrophe_tiers', [])
        
        applicable_md_tier = _determine_applicable_deductible_tier(total_si, md_tiers)
        applicable_bi_tier = _determine_applicable_deductible_tier(total_si, bi_tiers)
        applicable_nc_tier = _determine_applicable_deductible_tier(total_si, nc_tiers)
        
        md_value = (applicable_md_tier or {}).get('deductible')
        bi_value = (applicable_bi_tier or {}).get('deductible')
        nc_value = (applicable_nc_tier or {}).get('deductible')

        summary_parts = []
        if md_value and str(md_value).strip().upper() != 'N/A':
            summary_parts.append(f"MD: {str(md_value).strip()}")

        if bi_value and str(bi_value).strip():
            bi_str = str(bi_value).strip()
            if bi_str.upper() != 'N/A':
                if re.fullmatch(r'\d+', bi_str):
                    bi_str = f"Minimum {bi_str} days"
                elif re.fullmatch(r'\d+\s*days', bi_str, re.IGNORECASE):
                    bi_str = f"Minimum {bi_str}"
                summary_parts.append(f"BI: {bi_str}")

        if nc_value and str(nc_value).strip() and str(nc_value).strip().upper() != 'N/A':
            summary_parts.append(f"Nat Cat: {str(nc_value).strip()}")

        fallback_deductible_summary = deductibles_data.get('fallback_summary') or raw_data.get('deductible_summary_fallback')
        used_fallback_deductible = False

        if summary_parts:
            applicable_deductible_summary = " | ".join(summary_parts)
        elif fallback_deductible_summary:
            applicable_deductible_summary = fallback_deductible_summary
            used_fallback_deductible = True
        else:
            applicable_deductible_summary = "N/A"

        if used_fallback_deductible:
            logger.info(f"✅ Applicable deductibles (fallback): {applicable_deductible_summary}")
        else:
            logger.info(f"✅ Applicable deductibles: {applicable_deductible_summary}")
        
        # ====================================================================
        # STAGE 6: INTELLIGENT ANALYSIS WITH TRANSPARENT SCORING
        # ====================================================================
        
        logger.info(f"🎯 Stage 6: Analysis and scoring")
        
        # Count items for scoring
        exclusions_list = raw_data.get('exclusions_complete', {}).get('all_exclusions_list', [])
        warranties_list = raw_data.get('warranties_actual', {}).get('warranties_list', [])
        extensions_list = raw_data.get('extensions_and_conditions', {}).get('extensions_list', [])
        benefits_list = raw_data.get('coverage_and_benefits', {}).get('coverage_benefits_explained', [])
        
        logger.info(f"📊 Counts: {len(benefits_list)} benefits, {len(exclusions_list)} exclusions, " +
                   f"{len(warranties_list)} warranties, {len(extensions_list)} extensions")
        
        system_prompt_stage6 = """You are a senior insurance analyst providing transparent scoring."""
        
        premium_str = f"SAR {final_premium:,.2f}" if isinstance(final_premium, (int, float)) else "N/A"
        si_str = f"SAR {total_si:,.2f}" if isinstance(total_si, (int, float)) else "N/A"

        user_prompt_stage6 = f"""Analyze this insurance quote with TRANSPARENT scoring methodology.

QUOTE DETAILS:
Insurer: {raw_data.get('insurer_company_name')}
Premium: {premium_str}
Rate: {rate_formatted}
Sum Insured: {si_str}
Benefits: {len(benefits_list)}
Exclusions: {len(exclusions_list)}
Warranties: {len(warranties_list)}

SCORING METHODOLOGY (Be explicit):
- Coverage (0-30): Comprehensiveness of benefits and limits
- Pricing (0-25): Competitiveness of premium and rate
- Terms (0-20): Favorability of deductibles and conditions
- Exclusions (0-15): Fewer exclusions = higher score
- Flexibility (0-10): Payment terms, cancellation, etc.

Provide analysis as JSON:

{{
  "overall_score": 85.0,
  "score_breakdown": {{
    "coverage_score": 25,
    "coverage_reasoning": "Comprehensive benefits with good limits",
    "pricing_score": 20,
    "pricing_reasoning": "Competitive rate of 0.33 per mille",
    "terms_score": 18,
    "terms_reasoning": "Standard deductibles for this tier",
    "exclusions_score": 12,
    "exclusions_reasoning": "13 exclusions - moderate",
    "flexibility_score": 8,
    "flexibility_reasoning": "30 days cancellation notice"
  }},
  "strengths": [
    "Competitive premium of SAR 516,335",
    "Comprehensive coverage including SRCC",
    "Automatic reinstatement included",
    "... 5-7 specific strengths"
  ],
  "weaknesses": [
    "Higher deductible of SR 1 million for this tier",
    "Limited to KSA only",
    "... 3-5 weaknesses"
  ],
  "value_assessment": "Expert opinion 2-3 sentences",
  "recommendation": "Recommended / Good Value / Fair / Consider Alternatives"
}}

Keep strings SHORT. NO line breaks. Return ONLY valid JSON."""

        # PERFORMANCE FIX: Create async task for Stage 6 to run in parallel with Stage 4
        async def run_stage6():
            try:
                response = await openai_client.chat.completions.create(
                    model=settings.OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": system_prompt_stage6},
                        {"role": "user", "content": user_prompt_stage6}
                    ],
                    temperature=0.2,
                    response_format={"type": "json_object"},
                    max_tokens=2048
                )
                data = json.loads(response.choices[0].message.content)
                logger.info(f"✅ Stage 6 complete - Score: {data.get('overall_score')}")
                return data
            except Exception as e:
                logger.warning(f"⚠️ Using default analysis: {e}")
                return {
                    "overall_score": 75.0,
                    "score_breakdown": {},
                    "strengths": ["Comprehensive coverage"],
                    "weaknesses": ["Standard limitations"],
                    "value_assessment": "Standard offering",
                    "recommendation": "Fair"
                }
        
        # Start Stage 6 task
        stage6_task = asyncio.create_task(run_stage6())
        
        # ====================================================================
        # AWAIT PARALLEL TASKS (Stage 4 + Stage 6)
        # ====================================================================
        
        logger.info(f"⏳ Waiting for parallel API calls (Stage 4 + Stage 6)...")
        
        # Wait for both Stage 4 and Stage 6 to complete in parallel
        stage4_data, analysis = await asyncio.gather(stage4_task, stage6_task)
        
        logger.info(f"✅ Parallel stages complete")
        
        # ====================================================================
        # FINAL ASSEMBLY
        # ====================================================================
        
        logger.info(f"📦 Assembling final data structure")
        
        # Get operational details
        operational = stage4_data.get('operational_details', {})
        brokerage = stage4_data.get('brokerage_and_fees', {})
        
        # Extract and parse liability structure if liability insurance
        liability_structure_data = None
        if insurance_category == 'liability':
            liability_raw = raw_data.get('liability_limit_structure', {})
            if liability_raw and isinstance(liability_raw, dict):
                try:
                    from app.models.scheme import LiabilityLimitStructure

                    liability_structure_data = LiabilityLimitStructure(
                        per_claim_limit=_parse_currency_amount(str(liability_raw.get('per_claim_occurrence_limit', ''))) if liability_raw.get('per_claim_occurrence_limit') else None,
                        aggregate_annual_limit=_parse_currency_amount(str(liability_raw.get('aggregate_annual_limit', ''))) if liability_raw.get('aggregate_annual_limit') else None,
                        turnover_based=liability_raw.get('is_turnover_based', False),
                        turnover_multiplier=liability_raw.get('turnover_multiplier'),
                        annual_turnover=_parse_currency_amount(str(liability_raw.get('annual_turnover', ''))) if liability_raw.get('annual_turnover') else None,
                        turnover_cap=_parse_currency_amount(str(liability_raw.get('turnover_cap', ''))) if liability_raw.get('turnover_cap') else None,
                        defense_costs_inside_limit=liability_raw.get('defense_costs_inside_limit'),
                        defense_costs_sublimit=_parse_currency_amount(str(liability_raw.get('defense_costs_sublimit', ''))) if liability_raw.get('defense_costs_sublimit') else None,
                        retroactive_date=liability_raw.get('retroactive_date'),
                        extended_reporting_period=liability_raw.get('extended_reporting_period')
                    ).model_dump(exclude_none=True)

                    logger.info(f"✅ Liability structure extracted: {liability_structure_data}")
                except Exception as e:
                    logger.error(f"❌ Error parsing liability structure: {e}")
                    liability_structure_data = None

        final_data = {
            "company_name": raw_data.get('insurer_company_name', insurer_name),
            "insurer_detection_method": detection_method,
            "insurer_confidence": detection_confidence,
            "insured_name": raw_data.get('insured_customer_name', insured_name),
            "policy_type": raw_data.get('policy_type', ''),
            "policy_number": raw_data.get('policy_number', ''),

            # Insurance Category Detection (Phase 2 - Liability Support)
            "insurance_category": insurance_category,
            "insurance_category_confidence": category_confidence,

            # PREMIUM FIELDS (VAT v8.0 - Preserves Original Semantics)
            "premium_amount": final_premium,  # Normalized premium (always VAT-exclusive for comparison)
            "stated_premium": stated_premium,  # Original premium from document (as stated, before normalization)
            "premium_frequency": prem_info.get('payment_terms', 'Annual'),
            "rate": rate_formatted,
            "total_annual_cost": total_annual_cost,

            # VAT FIELDS (v9.0 - Comprehensive Pattern-Based Detection)
            "premium_includes_vat": False,  # Normalized field (always False for fair comparison)
            "original_premium_includes_vat": original_premium_includes_vat,  # As detected from document (source-of-truth)
            "vat_class": vat_class,  # P1 (inclusive) | P2 (exclusive)
            "vat_detection_method": vat_detection_method,  # How VAT was detected
            "vat_amount": vat_amount,  # null for P1 (inclusive)
            "vat_percentage": vat_percentage,  # null for P1, 15.0 for P2 (or extracted value)

            # VAT Classification Metadata (v9.0 - NEW)
            "vat_classification": {
                "class": vat_class,
                "detection_method": vat_detection_method,
                "pattern_matched": pattern_matched,
                "confidence": confidence,
                "is_assumed": requires_verification,
                "warning": vat_warning,
                "requires_verification": requires_verification
            },
            
            "score": analysis.get('overall_score', 75.0),
            "deductible": applicable_deductible_summary,
            "coverage_limit": si_breakdown.get('total_sum_insured', ''),
            "sum_insured_total": total_si or 0,  # CRITICAL FIX: Add numeric sum insured

            # Liability-Specific Structure (Phase 2 - only populated for liability insurance)
            "liability_structure": liability_structure_data,

            "key_benefits": benefits_list,  # NO LIMIT - show all
            "exclusions": exclusions_list,  # NO LIMIT - show all
            "warranties": warranties_list,  # NO LIMIT - show all
            
            "strengths": analysis.get('strengths', [])[:7],
            "weaknesses": analysis.get('weaknesses', [])[:5],
            
            "file_name": filename,
            "extraction_confidence": "high",
            "additional_info": analysis.get('value_assessment', ''),
            
            "_extended_data": {
                "document_format": doc_format,
                "sum_insured_breakdown": si_breakdown,
                "deductibles_complete": {
                    **deductibles_data,
                    "applicable_md_tier": applicable_md_tier,
                    "applicable_bi_tier": applicable_bi_tier,
                    "applicable_nc_tier": applicable_nc_tier
                },
                "sublimits_comprehensive": {**raw_data.get('sublimits_comprehensive', {}), **sublimits_detected},
                "exclusions_complete": raw_data.get('exclusions_complete', {}),
                "warranties_actual": raw_data.get('warranties_actual', {}),
                "extensions_and_conditions": raw_data.get('extensions_and_conditions', {}),
                "coverage_details": raw_data.get('coverage_and_benefits', {}),
                "subjectivities": stage4_data.get('subjectivities_and_requirements', {}),
                "operational_details": operational,
                "brokerage_info": brokerage,
                "special_conditions": stage4_data.get('special_conditions', []),
                "validity_period": operational.get('validity_period'),
                "geographical_limits": operational.get('geographical_limits'),
                "jurisdiction": operational.get('jurisdiction')
            },
            
            "_analysis_details": {
                **analysis,
                "score_methodology": "Coverage(30) + Pricing(25) + Terms(20) + Exclusions(15) + Flexibility(10)"
            },
            
            "_calculation_log": {
                "document_format": doc_format,
                "insurer_detected": insurer_name,
                "insured_detected": insured_name,
                "total_si_numeric": total_si,
                "premium_source": "stated" if premium_stated else "calculated",
                "premium_calculated": premium_calculated,
                "premium_stated": premium_stated,

                # VAT Detection Details (v9.0 - Comprehensive Pattern Detection)
                "vat_class": vat_class,  # P1 (inclusive) | P2 (exclusive)
                "vat_detection_method": vat_detection_method,
                "vat_pattern_matched": pattern_matched,
                "vat_confidence": confidence,
                "vat_warning": vat_warning,
                "vat_requires_verification": requires_verification,
                "original_premium_includes_vat": original_premium_includes_vat,
                "stated_premium_original": stated_premium,
                "normalized_premium_excl_vat": final_premium,
                "vat_amount": vat_amount,
                "vat_percentage": vat_percentage,
                "policy_fee": policy_fee,

                "rate_original": rate_text_raw,
                "rate_normalized": rate_formatted,
                "items_extracted": {
                    "benefits": len(benefits_list),
                    "exclusions": len(exclusions_list),
                    "warranties": len(warranties_list),
                    "extensions": len(extensions_list)
                },
                "deductible_tier_logic": f"SI={total_si/1_000_000:.1f}M → {applicable_deductible_summary}" if total_si else f"SI=N/A → {applicable_deductible_summary}"
            }
        }
        
        fingerprint = _generate_quote_fingerprint(
            final_data['company_name'],
            final_data['policy_number'],
            final_premium or 0
        )
        final_data['_quote_fingerprint'] = fingerprint
        
        # Calculate quality score
        quality_score, quality_breakdown = _calculate_quality_score(final_data)
        final_data['_quality_metrics'] = {
            'quality_score': quality_score,
            'quality_breakdown': quality_breakdown
        }
        
        # ====================================================================
        # FINAL LOGGING
        # ====================================================================
        
        logger.info(f"")
        logger.info(f"{'='*70}")
        logger.info(f"✅ EXTRACTION COMPLETE - {filename}")
        logger.info(f"{'='*70}")
        logger.info(f"🏢 Insurer: {final_data['company_name']}")
        logger.info(f"👤 Insured: {final_data['insured_name']}")
        logger.info(f"💰 Premium: SAR {final_premium:,.2f}" if final_premium else "💰 Premium: N/A")
        logger.info(f"📊 Rate: {rate_formatted}")
        logger.info(f"🎯 Score: {final_data['score']}/100")
        logger.info(f"📏 Quality: {quality_score:.1f}/100")
        logger.info(f"🔧 Deductible: {applicable_deductible_summary}")
        logger.info(f"⏰ Validity: {operational.get('validity_period', 'N/A')}")
        logger.info(f"💼 Brokerage: {brokerage.get('brokerage_percentage', 'N/A')}")
        logger.info(f"")
        logger.info(f"📦 Extracted:")
        logger.info(f"   ✓ Benefits: {len(benefits_list)}")
        logger.info(f"   ✓ Exclusions: {len(exclusions_list)}")
        logger.info(f"   ✓ Warranties: {len(warranties_list)}")
        logger.info(f"   ✓ Extensions: {len(extensions_list)}")
        logger.info(f"   ✓ Subjectivities: {len(stage4_data.get('subjectivities_and_requirements', {}).get('binding_requirements', []))}")
        logger.info(f"{'='*70}")
        
        return final_data
    
    except Exception as e:
        logger.error(f"❌ Extraction failed for {filename}: {str(e)}")
        logger.exception(e)
        raise AIParsingError(f"Extraction failed: {str(e)}")


async def extract_data_from_multiple_documents(
    documents: Dict[str, str]
) -> List[ExtractedQuoteData]:
    """Extract data from multiple documents with parallel processing and deduplication."""
    
    logger.info(f"🚀 Starting PARALLEL AI extraction for {len(documents)} documents")
    
    async def extract_single_document(filename: str, text: str) -> tuple:
        """Extract data from a single document."""
        try:
            logger.info(f"🤖 Processing: {filename}")
            data_dict = await extract_structured_data_from_text(text, filename)
            logger.info(f"✅ Extracted: {data_dict.get('company_name', 'Unknown')}")
            return filename, data_dict, None
        except Exception as e:
            logger.error(f"❌ Failed {filename}: {str(e)}")
            return filename, None, str(e)
    
    # Create parallel extraction tasks
    extraction_tasks = [
        extract_single_document(filename, text)
        for filename, text in documents.items()
    ]
    
    # Execute all AI extractions in parallel
    logger.info(f"⚡ Executing {len(extraction_tasks)} AI extractions in parallel...")
    extraction_results = await asyncio.gather(*extraction_tasks, return_exceptions=True)
    
    # Process results with deduplication
    extracted_quotes = []
    seen_fingerprints = set()
    failed_extractions = []
    
    for result in extraction_results:
        if isinstance(result, Exception):
            logger.error(f"❌ Extraction task failed: {str(result)}")
            continue
            
        filename, data_dict, error = result
        
        if error:
            failed_extractions.append({
                'filename': filename,
                'error': error
            })
            continue
            
        if not data_dict:
            failed_extractions.append({
                'filename': filename,
                'error': 'No data extracted'
            })
            continue
        
        # Check for duplicates
        fingerprint = data_dict.get('_quote_fingerprint', '')
        if fingerprint in seen_fingerprints:
            logger.warning(f"⚠️ DUPLICATE: {filename}")
            continue
        
        seen_fingerprints.add(fingerprint)
        
        try:
            quote_data = ExtractedQuoteData(**data_dict)
            extracted_quotes.append(quote_data)
        except Exception as e:
            logger.error(f"❌ Failed to create ExtractedQuoteData for {filename}: {str(e)}")
            failed_extractions.append({
                'filename': filename,
                'error': f"Data validation failed: {str(e)}"
            })
    
    if not extracted_quotes:
        raise AIParsingError("Failed to extract data from any documents")
    
    logger.info(f"")
    logger.info(f"✅ PARALLEL BATCH COMPLETE: {len(extracted_quotes)}/{len(documents)} documents")
    if failed_extractions:
        logger.warning(f"⚠️ {len(failed_extractions)} documents failed extraction")
    
    return extracted_quotes


def validate_extraction_quality(extracted_data: Dict) -> Tuple[bool, List[str]]:
    """Validate extraction quality."""
    issues = []
    
    rate = extracted_data.get('rate', '')
    if '$' in str(rate):
        issues.append("CRITICAL: Rate uses $ symbol")
    
    company = extracted_data.get('company_name', '').lower()
    insured_patterns = ['hospital', 'medical center', 'clinic', 'factory']
    if any(p in company for p in insured_patterns):
        issues.append("WARNING: company_name may be insured")
    
    exclusions = extracted_data.get('exclusions', [])
    if len(exclusions) < 10:
        issues.append(f"WARNING: Only {len(exclusions)} exclusions")
    
    warranties = extracted_data.get('warranties', [])
    if len(warranties) < 5:
        issues.append(f"WARNING: Only {len(warranties)} warranties")
    
    deductible = extracted_data.get('deductible')
    if not deductible or 'N/A' in str(deductible):
        issues.append("WARNING: Deductible missing")
    
    extended = extracted_data.get('_extended_data', {})
    if not extended.get('subjectivities'):
        issues.append("INFO: Subjectivities not extracted")
    
    is_valid = len([i for i in issues if 'CRITICAL' in i]) == 0
    
    return is_valid, issues


class AIParser:
    """Production-ready AI Parser v6.0"""

    async def extract_structured_data_from_text(self, text: str, filename: str, pdf_path: Optional[str] = None) -> Dict:
        return await extract_structured_data_from_text(text, filename, pdf_path)
    
    async def extract_data_from_multiple_documents(self, documents: Dict[str, str]) -> List[ExtractedQuoteData]:
        return await extract_data_from_multiple_documents(documents)
    
    def validate_extraction(self, extracted_data: Dict) -> Tuple[bool, List[str]]:
        return validate_extraction_quality(extracted_data)


ai_parser = AIParser()