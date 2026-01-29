"""
PRODUCTION AI RANKER & COMPARISON ENGINE v5.1 - PREMIUM & RATE FOCUSED
====================================================================
✅ Customizable weighted scoring system with user control
✅ Hakim Score integration for provider reputation (Official Saudi Market Rankings)
✅ Formula: 70% Quote Factors (Premium 25%, Rate 20%, Benefits 10%, Exclusions 5%, Warranties 4%, Extensions 4%, Subjectivities 2%) + 30% Hakim Score (Reputation & Financial Stability)
✅ Premium and Rate are the most important ranking factors
✅ Reputation & Financial Stability (Hakim Score) is MANDATORY - 30% weight
✅ Comprehensive Hakim Score database with official Saudi insurance market rankings
✅ Accurate formula with proper normalization and edge case handling
✅ AI-Powered Ranking Analysis - Intelligent insights and recommendations
✅ Client name prominently displayed in all outputs
✅ Correct badge assignment logic
✅ Full lists in side_by_side section
✅ Mixed insurance line warnings in comparison

Version: 6.0 - 70/30 Split: Quote Factors vs Hakim Score
Last Updated: 2025-01
"""

import json
import logging
import re
import uuid
import asyncio
from typing import List, Dict, Any, Set, Tuple, Optional
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from app.core.openai_client import openai_client
from app.core.config import settings
from app.models.quote_model import ExtractedQuoteData

logger = logging.getLogger(__name__)


class AIRankingError(Exception):
    """Custom exception for AI ranking failures."""
    pass


# ============================================================================
# HAKIM SCORE - OFFICIAL SAUDI INSURANCE PROVIDER RANKINGS
# Based on Official Saudi Insurance Market Data - Reputation & Financial Stability
# ============================================================================

HAKIM_SCORE = {
    # Premium Tier (0.96 score) - Highest Reputation & Financial Stability
    'The Company for Cooperative Insurance (Tawuniya)': {'score': 0.96, 'tier': 'Premium', 'rank': 1},
    'Tawuniya': {'score': 0.96, 'tier': 'Premium', 'rank': 1},
    'التعاونية': {'score': 0.96, 'tier': 'Premium', 'rank': 1},
    'Company for Cooperative Insurance': {'score': 0.96, 'tier': 'Premium', 'rank': 1},
    
    # Premium Tier (0.92 score)
    'Walaa Cooperative Insurance Company': {'score': 0.92, 'tier': 'Premium', 'rank': 2},
    'Walaa': {'score': 0.92, 'tier': 'Premium', 'rank': 2},
    'ولاء': {'score': 0.92, 'tier': 'Premium', 'rank': 2},
    
    'Mediterranean and Gulf Insurance': {'score': 0.92, 'tier': 'Premium', 'rank': 3},
    'MedGulf': {'score': 0.92, 'tier': 'Premium', 'rank': 3},
    'MedGulf Insurance': {'score': 0.92, 'tier': 'Premium', 'rank': 3},
    'ميدغلف': {'score': 0.92, 'tier': 'Premium', 'rank': 3},
    
    # Strong Tier (0.88 score) - High Reputation & Financial Stability
    'Gulf Insurance Group': {'score': 0.88, 'tier': 'Strong', 'rank': 4},
    'GIG': {'score': 0.88, 'tier': 'Strong', 'rank': 4},
    'Gulf Insurance Group (GIG)': {'score': 0.88, 'tier': 'Strong', 'rank': 4},
    'جي اي جي': {'score': 0.88, 'tier': 'Strong', 'rank': 4},
    
    'Gulf General Cooperative Insurance Company': {'score': 0.88, 'tier': 'Strong', 'rank': 5},
    'GGI': {'score': 0.88, 'tier': 'Strong', 'rank': 5},
    
    'Al-Etihad Cooperative Insurance Co.': {'score': 0.88, 'tier': 'Strong', 'rank': 6},
    'AL-Etihad': {'score': 0.88, 'tier': 'Strong', 'rank': 6},
    'Al Etihad': {'score': 0.88, 'tier': 'Strong', 'rank': 6},
    
    'Wataniya Insurance': {'score': 0.88, 'tier': 'Strong', 'rank': 7},
    'Wataniya': {'score': 0.88, 'tier': 'Strong', 'rank': 7},
    'الوطنية': {'score': 0.88, 'tier': 'Strong', 'rank': 7},
    'الوطنية للتأمين': {'score': 0.88, 'tier': 'Strong', 'rank': 7},
    
    # Solid Tier (0.84 score) - Good Reputation & Financial Stability
    'Malath Cooperative Insurance Company': {'score': 0.84, 'tier': 'Solid', 'rank': 8},
    'Malath': {'score': 0.84, 'tier': 'Solid', 'rank': 8},
    'Malath Cooperative Insurance': {'score': 0.84, 'tier': 'Solid', 'rank': 8},
    'ملاذ': {'score': 0.84, 'tier': 'Solid', 'rank': 8},
    'ملاذ للتأمين': {'score': 0.84, 'tier': 'Solid', 'rank': 8},
    
    'Liva Insurance Company': {'score': 0.84, 'tier': 'Solid', 'rank': 9},
    'Liva': {'score': 0.84, 'tier': 'Solid', 'rank': 9},
    'Liva Insurance': {'score': 0.84, 'tier': 'Solid', 'rank': 9},
    'ليفا': {'score': 0.84, 'tier': 'Solid', 'rank': 9},
    
    # Baseline Tier (0.8 score) - Standard Reputation & Financial Stability
    'Chubb Arabia Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 10},
    'Chubb': {'score': 0.8, 'tier': 'Baseline', 'rank': 10},
    'Chubb Arabia': {'score': 0.8, 'tier': 'Baseline', 'rank': 10},
    'تشب': {'score': 0.8, 'tier': 'Baseline', 'rank': 10},
    
    'Arabian Shield Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 11},
    'Arabian Shield': {'score': 0.8, 'tier': 'Baseline', 'rank': 11},
    
    'Allied Cooperative Insurance Group': {'score': 0.8, 'tier': 'Baseline', 'rank': 12},
    'ACIG': {'score': 0.8, 'tier': 'Baseline', 'rank': 12},
    
    'Saudi Arabian Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 13},
    'SAICO': {'score': 0.8, 'tier': 'Baseline', 'rank': 13},
    
    'Salama Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 14},
    'Salama': {'score': 0.8, 'tier': 'Baseline', 'rank': 14},
    'Salama Insurance': {'score': 0.8, 'tier': 'Baseline', 'rank': 14},
    'سلامة': {'score': 0.8, 'tier': 'Baseline', 'rank': 14},
    
    'Al Jazeera Takaful Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 15},
    'AJTC': {'score': 0.8, 'tier': 'Baseline', 'rank': 15},
    
    'Arab Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 16},
    'ACIC': {'score': 0.8, 'tier': 'Baseline', 'rank': 16},
    'Arabia Insurance Cooperative Company (AICC)': {'score': 0.8, 'tier': 'Baseline', 'rank': 16},
    'Arabia Insurance Cooperative Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 16},
    'AICC': {'score': 0.8, 'tier': 'Baseline', 'rank': 16},
    'العربية للتأمين': {'score': 0.8, 'tier': 'Baseline', 'rank': 16},
    
    'Al-Sagr Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 17},
    'Al-Sagr': {'score': 0.8, 'tier': 'Baseline', 'rank': 17},
    'Al Sagr': {'score': 0.8, 'tier': 'Baseline', 'rank': 17},  # No hyphen variant
    'Al Sagr Co-operative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 17},  # Co-operative spelling
    'Al Sagr Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 17},  # No hyphen, Cooperative
    'AlSagr': {'score': 0.8, 'tier': 'Baseline', 'rank': 17},  # Combined spelling
    
    'Amanah Cooperative Insurance Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 18},
    'Amanah': {'score': 0.8, 'tier': 'Baseline', 'rank': 18},
    
    'Mutakamela Insurance': {'score': 0.8, 'tier': 'Baseline', 'rank': 19},
    'Mutakamela': {'score': 0.8, 'tier': 'Baseline', 'rank': 19},
    
    # Al Rajhi Takaful (from spreadsheet)
    'Al Rajhi Takaful': {'score': 0.8, 'tier': 'Baseline', 'rank': 20},
    'ART': {'score': 0.8, 'tier': 'Baseline', 'rank': 20},
    'Al Rajhi Takaful Company': {'score': 0.8, 'tier': 'Baseline', 'rank': 20},
    
    # Challenged Tier (0.72 score) - Lower Reputation & Financial Stability
    'Gulf Union Cooperative Insurance Company': {'score': 0.72, 'tier': 'Challenged', 'rank': 21},
    'Gulf Union': {'score': 0.72, 'tier': 'Challenged', 'rank': 21},
    
    'United Cooperative Assurance Company': {'score': 0.72, 'tier': 'Challenged', 'rank': 22},
    'UCA': {'score': 0.72, 'tier': 'Challenged', 'rank': 22},
    'United Cooperative Assurance (UCA)': {'score': 0.72, 'tier': 'Challenged', 'rank': 22},
    'المتحدة': {'score': 0.72, 'tier': 'Challenged', 'rank': 22},
    'المتحدة للتأمين': {'score': 0.72, 'tier': 'Challenged', 'rank': 22},
    
    # International providers - Strong Tier (0.85-0.87 score)
    'AXA Cooperative Insurance Company': {'score': 0.85, 'tier': 'Strong', 'rank': 23},
    'AXA': {'score': 0.85, 'tier': 'Strong', 'rank': 23},
    'AXA Gulf': {'score': 0.85, 'tier': 'Strong', 'rank': 23},
    'أكسا': {'score': 0.85, 'tier': 'Strong', 'rank': 23},
    
    'Allianz Saudi Fransi Cooperative Insurance Company': {'score': 0.86, 'tier': 'Strong', 'rank': 24},
    'Allianz': {'score': 0.86, 'tier': 'Strong', 'rank': 24},
    'Allianz Saudi Fransi': {'score': 0.86, 'tier': 'Strong', 'rank': 24},
    'أليانز': {'score': 0.86, 'tier': 'Strong', 'rank': 24},
    
    'Zurich Insurance': {'score': 0.87, 'tier': 'Strong', 'rank': 25},
    'Zurich': {'score': 0.87, 'tier': 'Strong', 'rank': 25},
    'زيورخ': {'score': 0.87, 'tier': 'Strong', 'rank': 25},
    
    'Tokio Marine Saudi Arabia': {'score': 0.83, 'tier': 'Solid', 'rank': 26},
    'Tokio Marine': {'score': 0.83, 'tier': 'Solid', 'rank': 26},
    'توكيو مارين': {'score': 0.83, 'tier': 'Solid', 'rank': 26},
}

# Default score for providers not in Hakim Score
DEFAULT_HAKIM_SCORE = 0.75
DEFAULT_TIER = 'Standard'


# ============================================================================
# DEFAULT WEIGHTS v6.0 - 70/30 Split: Quote Factors vs Hakim Score
# 70% from Quote Factors (Premium, Rate, Benefits, Exclusions, Warranties, Extensions, Subjectivities)
# 30% from Hakim Score (Reputation & Financial Stability)
# ============================================================================

DEFAULT_WEIGHTS = {
    # Quote Factors (70% total)
    'premium': 0.25,        # Premium - 25% (most important quote factor)
    'rate': 0.20,           # Rate - 20% (second most important quote factor)
    'benefits': 0.10,       # Benefits - 10% (more benefits = better)
    'exclusions': 0.05,     # Exclusions - 5% (fewer exclusions = better)
    'warranties': 0.04,     # Warranties - 4% (fewer warranties = better)
    'extensions': 0.04,     # Extensions - 4% (more extensions = better)
    'subjectivities': 0.02, # Subjectivities - 2% (fewer subjectivities = better)

    # Hakim Score (30% total)
    'provider_reputation': 0.30,  # MANDATORY - Reputation & Financial Stability (Hakim Score) - 30%

    # Total: 1.00 (100%)
    # NOTE: 70% from quote factors, 30% from Hakim Score (reputation & financial stability)
}


# ============================================================================
# LIABILITY INSURANCE WEIGHTS v1.0 - Coverage-Limit-Aware Scoring
# Different from property: Higher coverage limits are BETTER (more protection)
# ============================================================================

LIABILITY_WEIGHTS = {
    # Coverage Factors (40% total) - Emphasize coverage adequacy
    'per_claim_coverage': 0.20,      # Higher per-claim limit = better protection
    'aggregate_coverage': 0.10,      # Higher aggregate vs per-claim = better
    'defense_costs_bonus': 0.08,     # Defense costs outside limit = better
    'coverage_breadth': 0.02,        # Benefits, extensions

    # Pricing Factors (25% total) - Premium efficiency, not absolute premium
    'premium_efficiency': 0.15,      # Premium per SAR 1M of coverage (lower = better)
    'rate': 0.10,                    # Rate still matters but less weight

    # Quality Factors (5% total)
    'exclusions': 0.03,              # Fewer exclusions = better
    'warranties': 0.02,              # Fewer warranties = better

    # Hakim Score (30% total) - Same as property
    'provider_reputation': 0.30,     # Provider reputation and financial stability

    # Total: 100% (0.20+0.10+0.08+0.02+0.15+0.10+0.03+0.02+0.30 = 1.00)
}

logger.info("Liability scoring weights configured: Coverage-limit-aware evaluation")


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _normalize_rate(rate_value: Any) -> str:
    """Normalize rate preserving ‰ or % from parser."""
    if rate_value is None:
        return "N/A"
    if isinstance(rate_value, str):
        return rate_value.strip() if rate_value.strip() else "N/A"
    if isinstance(rate_value, (int, float)):
        return f"{float(rate_value):.2f}‰"
    return "N/A"


def _extract_rate_value(rate_value: Any) -> float:
    """Extract numeric rate value from string or number for calculations."""
    if rate_value is None:
        return 0.0
    if isinstance(rate_value, (int, float)):
        return float(rate_value)
    if isinstance(rate_value, str):
        try:
            # Remove ‰, %, and whitespace
            clean = rate_value.replace("‰", "").replace("%", "").strip()
            return float(clean) if clean else 0.0
        except:
            return 0.0
    return 0.0


def _normalize_premium(premium_value: Any) -> float:
    """Normalize premium to float."""
    if premium_value is None:
        return 0.0
    if isinstance(premium_value, (int, float)):
        return float(premium_value)
    if isinstance(premium_value, str):
        try:
            clean = premium_value.replace("SAR", "").replace("SR", "").replace("$", "").replace(",", "").strip()
            return float(clean) if clean else 0.0
        except:
            return 0.0
    return 0.0


def _get_normalized_premium_for_comparison(quote: ExtractedQuoteData) -> float:
    """
    Get premium amount normalized for fair comparison across different VAT classes.

    VAT Class Handling:
    - P1 (VAT-inclusive): Use stated premium as-is (VAT already included)
    - P2 (VAT-exclusive): Use stated premium as-is (VAT shown separately)
    - P3 (VAT-deferred): Use stated premium as-is (VAT charged at billing)

    Returns:
        Premium amount for comparison purposes
    """
    premium = _normalize_premium(quote.premium_amount) or 0

    # P3 handling: Use stated premium (VAT not in quote)
    if hasattr(quote, 'vat_class') and quote.vat_class == "P3":
        logger.info(f"P3 quote ({quote.company_name}): Using stated premium {premium} (VAT deferred to billing)")
        return premium

    # P1 handling: Use stated premium (VAT already included)
    if hasattr(quote, 'vat_class') and quote.vat_class == "P1":
        logger.info(f"P1 quote ({quote.company_name}): Using stated premium {premium} (VAT inclusive)")
        return premium

    # P2 handling: Use stated premium (VAT shown separately)
    if hasattr(quote, 'vat_class') and quote.vat_class == "P2":
        logger.info(f"P2 quote ({quote.company_name}): Using stated premium {premium} (VAT exclusive)")
        return premium

    # Fallback: If premium_amount is None/0 but total_annual_cost exists,
    # extract base premium (this shouldn't happen after parser fix, but safety check)
    if premium == 0 and quote.total_annual_cost:
        # Assume 15% VAT and try to extract base
        total = _normalize_premium(quote.total_annual_cost)
        if total > 0:
            # Approximate: total = base * 1.15, so base = total / 1.15
            premium = total / 1.15
            logger.warning(f"⚠️ Extracted base premium from total for {quote.company_name}")

    return premium


async def _get_hakim_score_from_db(company_name: str) -> Optional[Tuple[float, str, int]]:
    """
    Get Hakim Score from database with intelligent company name matching.
    Handles variations like "GIG" matching "Gulf Insurance Group (GIG)".
    Returns: (score, tier, rank) or None if not found
    """
    try:
        from app.services.hakim_score_service import hakim_score_service
        
        # Check if service is connected
        if not hakim_score_service.collection:
            return None
        
        # Use intelligent matching (handles variations, aliases, abbreviations)
        db_score = await hakim_score_service.get_hakim_score(company_name)
        
        if db_score:
            db_score_value = db_score.get('score', 0.0)
            # Score of 0.0 is valid (explicitly set by admin to disable)
            score = db_score_value * 100  # Convert to 0-100 scale
            tier = db_score.get('tier', 'Standard')
            rank = db_score.get('rank', 999)
            logger.debug(f"✅ DB match: {company_name} → {db_score['company_name']} (score: {score:.1f}, tier: {tier})")
            return score, tier, rank
        
        return None
        
    except Exception as e:
        logger.debug(f"⚠️ Could not get Hakim score from DB for {company_name}: {e}")
        return None


def _get_hakim_score(company_name: str, ia_compliant: bool = False) -> Tuple[float, str, int]:
    """
    Get Hakim Score for provider.
    NEW LOGIC: Checks database first, then falls back to hardcoded values.
    Handles variations in company name format.
    Returns: (score, tier, rank)
    
    NOTE: This is a synchronous function, but it uses async DB lookup internally.
    For production use, prefer async version: _get_hakim_score_async()
    """
    import asyncio
    
    # Try to get from database first (sync wrapper for async call)
    try:
        # Check if we're in an async context
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Try to get from DB (non-blocking if possible)
        if loop.is_running():
            # If loop is running, we can't use run_until_complete
            # Fall back to hardcoded values
            logger.debug(f"⚠️ Event loop running, using hardcoded Hakim scores for {company_name}")
        else:
            db_result = loop.run_until_complete(_get_hakim_score_from_db(company_name))
            if db_result:
                score, tier, rank = db_result
                if ia_compliant:
                    score += 3
                logger.debug(f"✅ Got Hakim score from DB: {company_name} = {score:.1f} ({tier})")
                return score, tier, rank
    except Exception as e:
        logger.debug(f"⚠️ DB lookup failed for {company_name}, using hardcoded: {e}")
    
    # FALLBACK: Use hardcoded values (original logic)
    # Try exact match first
    hakim_data = HAKIM_SCORE.get(company_name)
    
    if hakim_data:
        score = hakim_data['score'] * 100
        tier = hakim_data['tier']
        rank = hakim_data.get('rank', 999)
        
        if ia_compliant:
            score += 3
        
        return score, tier, rank
    
    # Try case-insensitive lookup
    company_lower = company_name.lower().strip()
    company_normalized = company_lower.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
    
    for key, value in HAKIM_SCORE.items():
        key_lower = key.lower().strip()
        key_normalized = key_lower.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
        
        # Exact match (case-insensitive)
        if key_lower == company_lower:
            score = value['score'] * 100
            tier = value['tier']
            rank = value.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
        
        # Normalized match (handles spacing/punctuation differences)
        if key_normalized == company_normalized:
            score = value['score'] * 100
            tier = value['tier']
            rank = value.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
        
        # Check if company name contains key or vice versa
        if company_lower in key_lower or key_lower in company_lower:
            if len(company_lower) >= 3 and len(key_lower) >= 3:  # Avoid false positives
                score = value['score'] * 100
                tier = value['tier']
                rank = value.get('rank', 999)
                
                if ia_compliant:
                    score += 3
                
                return score, tier, rank
    
    # Try abbreviation matching (e.g., "GIG" matches "Gulf Insurance Group")
    if len(company_name) <= 5 and company_name.isupper():
        for key, value in HAKIM_SCORE.items():
            # Extract first letters of words
            key_words = key.split()
            key_abbrev = "".join([w[0].upper() for w in key_words if w and w[0].isalpha()])
            
            if company_name == key_abbrev:
                score = value['score'] * 100
                tier = value['tier']
                rank = value.get('rank', 999)
                
                if ia_compliant:
                    score += 3
                
                return score, tier, rank
            
            # Check if abbreviation appears in the key
            if company_name in key.upper():
                score = value['score'] * 100
                tier = value['tier']
                rank = value.get('rank', 999)
                
                if ia_compliant:
                    score += 3
                
                return score, tier, rank
    
    # Try partial match for common variations
    # UCA variations
    if 'uca' in company_lower or 'united cooperative' in company_lower:
        uca_data = HAKIM_SCORE.get('United Cooperative Assurance (UCA)') or HAKIM_SCORE.get('UCA')
        if uca_data:
            score = uca_data['score'] * 100
            tier = uca_data['tier']
            rank = uca_data.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
    
    # Default score for providers not in Hakim Score
    score = DEFAULT_HAKIM_SCORE * 100
    if ia_compliant:
        score += 3
    
    return score, DEFAULT_TIER, 999


async def _get_hakim_score_async(company_name: str, ia_compliant: bool = False) -> Tuple[float, str, int]:
    """
    Async version of _get_hakim_score.
    Checks database first, then falls back to hardcoded values.
    This is the preferred method for async contexts.
    """
    # Try database first
    db_result = await _get_hakim_score_from_db(company_name)
    if db_result:
        score, tier, rank = db_result
        if ia_compliant:
            score += 3
        logger.debug(f"✅ Got Hakim score from DB: {company_name} = {score:.1f} ({tier})")
        return score, tier, rank
    
    # Fallback to hardcoded values
    hakim_data = HAKIM_SCORE.get(company_name)
    
    if hakim_data:
        score = hakim_data['score'] * 100
        tier = hakim_data['tier']
        rank = hakim_data.get('rank', 999)
        
        if ia_compliant:
            score += 3
        
        return score, tier, rank
    
    # Try case-insensitive lookup
    company_lower = company_name.lower().strip()
    company_normalized = company_lower.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
    
    for key, value in HAKIM_SCORE.items():
        key_lower = key.lower().strip()
        key_normalized = key_lower.replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
        
        # Exact match (case-insensitive)
        if key_lower == company_lower:
            score = value['score'] * 100
            tier = value['tier']
            rank = value.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
        
        # Normalized match
        if key_normalized == company_normalized:
            score = value['score'] * 100
            tier = value['tier']
            rank = value.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
    
    # Try abbreviation matching
    if len(company_name) <= 5 and company_name.isupper():
        for key, value in HAKIM_SCORE.items():
            key_words = key.split()
            key_abbrev = "".join([w[0].upper() for w in key_words if w and w[0].isalpha()])
            
            if company_name == key_abbrev:
                score = value['score'] * 100
                tier = value['tier']
                rank = value.get('rank', 999)
                
                if ia_compliant:
                    score += 3
                
                return score, tier, rank
    
    # GIG variations
    if company_lower == 'gig' or company_normalized == 'gig':
        gig_data = HAKIM_SCORE.get('Gulf Insurance Group (GIG)') or HAKIM_SCORE.get('GIG')
        if gig_data:
            score = gig_data['score'] * 100
            tier = gig_data['tier']
            rank = gig_data.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
    
    # Try fuzzy matching with similarity threshold
    from difflib import SequenceMatcher
    
    def normalize_for_match(name: str) -> str:
        """Normalize name for matching"""
        return name.lower().replace(' ', '').replace('-', '').replace('.', '').replace('(', '').replace(')', '')
    
    company_norm = normalize_for_match(company_name)
    best_match = None
    best_similarity = 0.0
    
    for key, value in HAKIM_SCORE.items():
        key_norm = normalize_for_match(key)
        similarity = SequenceMatcher(None, company_norm, key_norm).ratio()
        
        if similarity > best_similarity and similarity >= 0.7:  # 70% similarity threshold
            best_similarity = similarity
            best_match = value
    
    if best_match:
        score = best_match['score'] * 100
        tier = best_match['tier']
        rank = best_match.get('rank', 999)
        
        if ia_compliant:
            score += 3
        
        logger.debug(f"✅ Fuzzy match: {company_name} (similarity: {best_similarity:.2f})")
        return score, tier, rank
    
    # Try partial match for common variations
    if 'uca' in company_lower or 'united cooperative' in company_lower:
        uca_data = HAKIM_SCORE.get('United Cooperative Assurance (UCA)') or HAKIM_SCORE.get('UCA')
        if uca_data:
            score = uca_data['score'] * 100
            tier = uca_data['tier']
            rank = uca_data.get('rank', 999)
            
            if ia_compliant:
                score += 3
            
            return score, tier, rank
    
    # Default score
    score = DEFAULT_HAKIM_SCORE * 100
    if ia_compliant:
        score += 3
    
    return score, DEFAULT_TIER, 999


def _calculate_weighted_score(
    quote: ExtractedQuoteData,
    all_premiums: List[float],
    all_rates: List[float],
    all_benefits_counts: List[int],
    all_exclusions_counts: List[int],
    all_warranties_counts: List[int],
    all_extensions_counts: List[int],
    all_subjectivities_counts: List[int],
    weights: Dict[str, float]
) -> Tuple[float, Dict[str, float]]:
    """
    Calculate weighted score based on user-defined weights with Hakim Score.
    Premium and Rate are the most important factors.
    Uses proportional pricing penalties instead of harsh zero-scoring.
    Returns: (total_score, score_breakdown)
    """
    premium = _get_normalized_premium_for_comparison(quote)
    rate_value = _extract_rate_value(quote.rate)
    benefits_count = len(quote.key_benefits or [])
    exclusions_count = len(quote.exclusions or [])
    warranties_count = len(quote.warranties or [])
    
    # Extract extensions
    extended = _get_extended_data(quote)
    extensions_data = extended.get('extensions_and_conditions', {})
    extensions_list = extensions_data.get('extensions_list', []) if isinstance(extensions_data, dict) else []
    extensions_count = len(extensions_list)
    
    # Extract subjectivities
    subjectivities = _extract_subjectivities(quote)
    subjectivities_count = len(subjectivities)
    
    ia_compliant = getattr(quote, 'ia_compliant', False)
    
    score_breakdown = {}
    
    # STEP 1: Calculate base coverage/quality score (benefits, exclusions, warranties, extensions, subjectivities)
    # This represents the quality of coverage, independent of pricing
    
    # BENEFITS SCORE (10% weight - part of 70% quote factors)
    # More benefits = higher score
    if all_benefits_counts and max(all_benefits_counts) > 0:
        benefits_score = (benefits_count / max(all_benefits_counts)) * 100
    else:
        benefits_score = 50 if benefits_count > 0 else 30
    score_breakdown['benefits'] = benefits_score * weights['benefits']
    
    # EXCLUSIONS SCORE (5% weight - part of 70% quote factors)
    # Fewer exclusions = higher score
    if all_exclusions_counts and max(all_exclusions_counts) > 0:
        exclusions_score = 100 - ((exclusions_count / max(all_exclusions_counts)) * 100)
    else:
        exclusions_score = 50 if exclusions_count == 0 else 30
    score_breakdown['exclusions'] = exclusions_score * weights['exclusions']
    
    # WARRANTIES SCORE (4% weight - part of 70% quote factors)
    # Fewer warranties = higher score (warranties are restrictions)
    if all_warranties_counts and max(all_warranties_counts) > 0:
        warranties_score = 100 - ((warranties_count / max(all_warranties_counts)) * 100)
    else:
        warranties_score = 50 if warranties_count == 0 else 30
    score_breakdown['warranties'] = warranties_score * weights['warranties']
    
    # EXTENSIONS SCORE (4% weight - part of 70% quote factors)
    # More extensions = higher score (extensions are additional coverage)
    if all_extensions_counts and max(all_extensions_counts) > 0:
        extensions_score = (extensions_count / max(all_extensions_counts)) * 100
    else:
        extensions_score = 50 if extensions_count > 0 else 30
    score_breakdown['extensions'] = extensions_score * weights['extensions']
    
    # SUBJECTIVITIES SCORE (2% weight - part of 70% quote factors)
    # Fewer subjectivities = higher score (subjectivities are requirements/restrictions)
    if all_subjectivities_counts and max(all_subjectivities_counts) > 0:
        subjectivities_score = 100 - ((subjectivities_count / max(all_subjectivities_counts)) * 100)
    else:
        subjectivities_score = 50 if subjectivities_count == 0 else 30
    score_breakdown['subjectivities'] = subjectivities_score * weights['subjectivities']
    
    # Calculate base coverage score (sum of coverage factors)
    base_coverage_score = (
        score_breakdown['benefits'] +
        score_breakdown['exclusions'] +
        score_breakdown['warranties'] +
        score_breakdown['extensions'] +
        score_breakdown['subjectivities']
    )
    
    # STEP 2: Calculate premium penalty (proportional, capped to prevent excessive impact)
    # Premium penalty should be proportional to how much higher the premium is relative to the lowest
    premium_penalty = 0.0
    premium_score_raw = 100.0  # Start with full score
    
    # PRODUCTION FIX: Filter out invalid premiums (0 or negative) before calculations
    if all_premiums and len(all_premiums) > 0:
        valid_premiums = [p for p in all_premiums if p > 0]
        
        if valid_premiums and len(valid_premiums) > 1 and max(valid_premiums) > min(valid_premiums):
            premium_range = max(valid_premiums) - min(valid_premiums)
            min_premium = min(valid_premiums)
            
            # PRODUCTION FIX: Explicit zero-check before division to prevent crashes
            if premium > 0 and min_premium > 0 and premium_range > 0 and premium > min_premium:
                # Calculate percentage premium difference
                premium_diff_pct = ((premium - min_premium) / min_premium) * 100
                
                # Apply proportional penalty: 13% higher premium = ~7-8 point penalty (capped at 30 points)
                # Formula: penalty = min(30, premium_diff_pct * 0.6)
                # This means 13% higher premium = ~7.8 point penalty
                premium_penalty = min(30.0, premium_diff_pct * 0.6)
                premium_score_raw = 100.0 - premium_penalty
            elif premium > 0 and premium == min_premium:
                # This quote has the lowest premium - full score
                premium_score_raw = 100.0
            elif premium > 0:
                # Valid premium but some edge case
                premium_score_raw = 100.0
            else:
                # Current quote has no valid premium - penalty
                premium_score_raw = 30.0
                logger.warning(f"⚠️ {quote.company_name}: Invalid premium ({premium}), applying penalty score")
        elif valid_premiums and len(valid_premiums) == 1:
            # Only one valid premium across all quotes - no penalty possible
            premium_score_raw = 100.0 if premium > 0 else 30.0
        else:
            # No valid premiums found in any quote - default score
            premium_score_raw = 50.0
            logger.warning(f"⚠️ No valid premiums found in comparison set")
    else:
        # No premiums data at all
        premium_score_raw = 50.0
    
    # Ensure premium score is never below 0
    premium_score_raw = max(0.0, premium_score_raw)
    score_breakdown['premium'] = premium_score_raw * weights['premium']
    
    # STEP 3: Calculate rate penalty (proportional, capped to prevent excessive impact)
    rate_penalty = 0.0
    rate_score_raw = 100.0  # Start with full score
    
    if all_rates and len(all_rates) > 0:
        # PRODUCTION FIX: Filter out invalid rates (0 or negative) before calculations
        valid_rates = [r for r in all_rates if r > 0]
        
        if valid_rates and len(valid_rates) > 1 and max(valid_rates) > min(valid_rates):
            rate_range = max(valid_rates) - min(valid_rates)
            min_rate = min(valid_rates)
            
            # PRODUCTION FIX: Explicit zero-check before division to prevent crashes
            if rate_value > 0 and min_rate > 0 and rate_range > 0 and rate_value > min_rate:
                # Calculate percentage rate difference
                rate_diff_pct = ((rate_value - min_rate) / min_rate) * 100
                
                # Apply proportional penalty: capped at 25 points
                # Formula: penalty = min(25, rate_diff_pct * 0.5)
                rate_penalty = min(25.0, rate_diff_pct * 0.5)
                rate_score_raw = 100.0 - rate_penalty
            elif rate_value > 0 and rate_value == min_rate:
                # This quote has the lowest rate - full score
                rate_score_raw = 100.0
            elif rate_value > 0:
                # Valid rate but some edge case
                rate_score_raw = 100.0
            else:
                # Current quote has no valid rate - penalty
                rate_score_raw = 30.0
                logger.warning(f"⚠️ {quote.company_name}: Invalid rate ({rate_value}), applying penalty score")
        elif valid_rates and len(valid_rates) == 1:
            # Only one valid rate across all quotes - no penalty possible
            rate_score_raw = 100.0 if rate_value > 0 else 30.0
        else:
            # No valid rates found in any quote - default score
            rate_score_raw = 50.0
            logger.warning(f"⚠️ No valid rates found in comparison set")
    else:
        # No rates data at all
        rate_score_raw = 50.0
    
    # Ensure rate score is never below 0
    rate_score_raw = max(0.0, rate_score_raw)
    score_breakdown['rate'] = rate_score_raw * weights['rate']
    
    # STEP 4: Hakim Score contribution (30% weight - MANDATORY)
    # This is 30% of total - ensures companies with better reputation and financial stability rank higher
    # NOTE: This function is sync, so we use sync version (which tries DB first, then falls back)
    hakim_score, hakim_tier, hakim_rank = _get_hakim_score(quote.company_name, ia_compliant)
    # Hakim Score is already 0-100, so we use it directly
    reputation_score = hakim_score  # Already normalized to 0-100
    score_breakdown['provider_reputation'] = reputation_score * weights['provider_reputation']
    score_breakdown['hakim_score'] = hakim_score
    score_breakdown['hakim_tier'] = hakim_tier
    score_breakdown['hakim_rank'] = hakim_rank
    
    # STEP 5: Calculate final weighted score
    # Formula: base_coverage_score + premium_score + rate_score + hakim_score
    # This ensures all components contribute proportionally
    total_score = (
        base_coverage_score +
        score_breakdown['premium'] +
        score_breakdown['rate'] +
        score_breakdown['provider_reputation']
    )
    
    # Ensure score is between 0 and 100
    total_score = max(0.0, min(100.0, total_score))

    return round(total_score, 2), score_breakdown


def _calculate_liability_score(
    quote: ExtractedQuoteData,
    all_quotes: List[ExtractedQuoteData],
    weights: Dict[str, float] = None
) -> Tuple[float, Dict[str, float]]:
    """
    Calculate weighted score for LIABILITY insurance quotes.

    KEY DIFFERENCES FROM PROPERTY INSURANCE:
    - Sum insured represents COVERAGE LIMIT (exposure to claims), not asset value
    - Higher coverage limits are BETTER (more protection)
    - Premium evaluated relative to coverage provided (premium per SAR 1M)
    - Defense costs outside limit is a significant advantage
    - Aggregate limits provide additional protection beyond per-claim

    Args:
        quote: Quote to score
        all_quotes: All liability quotes in comparison set
        weights: Custom weights (defaults to LIABILITY_WEIGHTS)

    Returns:
        (total_score, score_breakdown)
    """
    if weights is None:
        weights = LIABILITY_WEIGHTS

    score_breakdown = {}

    # Extract liability structure
    liability = getattr(quote, 'liability_structure', None)

    # COMPONENT 1: PER-CLAIM COVERAGE SCORE (20%)
    # Higher per-claim limit = better protection for single large claims
    if liability and hasattr(liability, 'per_claim_limit') and liability.per_claim_limit:
        per_claim_limit = float(liability.per_claim_limit)
    else:
        # Fallback to sum_insured_total
        per_claim_limit = float(quote.sum_insured_total) if quote.sum_insured_total else 0.0

    # Get all per-claim limits for normalization
    all_per_claim_limits = []
    for q in all_quotes:
        q_liability = getattr(q, 'liability_structure', None)
        if q_liability and hasattr(q_liability, 'per_claim_limit') and q_liability.per_claim_limit:
            all_per_claim_limits.append(float(q_liability.per_claim_limit))
        elif q.sum_insured_total:
            all_per_claim_limits.append(float(q.sum_insured_total))

    if all_per_claim_limits and max(all_per_claim_limits) > 0:
        # Normalize: Higher limit = higher score
        per_claim_score = (per_claim_limit / max(all_per_claim_limits)) * 100
    else:
        per_claim_score = 50  # Default

    score_breakdown['per_claim_coverage'] = per_claim_score * weights['per_claim_coverage']
    logger.debug(f"  Per-Claim Coverage: {per_claim_score:.1f} (limit: SAR {per_claim_limit:,.0f})")

    # COMPONENT 2: AGGREGATE COVERAGE BONUS (10%)
    # If aggregate > per-claim, provider offers more total coverage
    aggregate_bonus_score = 50  # Default neutral

    if liability and hasattr(liability, 'aggregate_annual_limit') and liability.aggregate_annual_limit:
        aggregate_limit = float(liability.aggregate_annual_limit)

        if per_claim_limit > 0:
            aggregate_ratio = aggregate_limit / per_claim_limit

            # Score based on ratio:
            # 1.0x (aggregate = per-claim) = 50 points
            # 2.0x = 75 points
            # 5.0x = 100 points
            if aggregate_ratio >= 5.0:
                aggregate_bonus_score = 100
            elif aggregate_ratio >= 2.0:
                aggregate_bonus_score = 50 + ((aggregate_ratio - 2.0) / 3.0) * 50
            elif aggregate_ratio >= 1.0:
                aggregate_bonus_score = 50 + ((aggregate_ratio - 1.0) / 1.0) * 25
            else:
                # Aggregate < per-claim is unusual but possible
                aggregate_bonus_score = 30

            logger.debug(f"  Aggregate Bonus: {aggregate_bonus_score:.1f} (ratio: {aggregate_ratio:.1f}x)")

    score_breakdown['aggregate_coverage'] = aggregate_bonus_score * weights['aggregate_coverage']

    # COMPONENT 3: DEFENSE COSTS BONUS (8%)
    # Defense costs outside limit = better coverage
    defense_costs_score = 50  # Default unknown

    if liability and hasattr(liability, 'defense_costs_inside_limit'):
        if liability.defense_costs_inside_limit is False:
            # Defense costs OUTSIDE limit (better)
            defense_costs_score = 100
            logger.debug(f"  Defense Costs: Outside limit (BEST) = {defense_costs_score}")
        elif liability.defense_costs_inside_limit is True:
            # Defense costs INSIDE limit (reduces available coverage)
            defense_costs_score = 40
            logger.debug(f"  Defense Costs: Inside limit (reduces coverage) = {defense_costs_score}")

    score_breakdown['defense_costs_bonus'] = defense_costs_score * weights['defense_costs_bonus']

    # COMPONENT 4: COVERAGE BREADTH (7%)
    # Same as property: benefits, extensions
    benefits_count = len(quote.key_benefits or [])
    all_benefits_counts = [len(q.key_benefits or []) for q in all_quotes]

    if all_benefits_counts and max(all_benefits_counts) > 0:
        coverage_breadth_score = (benefits_count / max(all_benefits_counts)) * 100
    else:
        coverage_breadth_score = 50

    score_breakdown['coverage_breadth'] = coverage_breadth_score * weights['coverage_breadth']

    # COMPONENT 5: PREMIUM EFFICIENCY (15%)
    # Premium per SAR 1M of coverage (lower = better value)
    premium = _normalize_premium(quote.premium_amount) or 0

    if per_claim_limit > 0 and premium > 0:
        premium_per_million = (premium / per_claim_limit) * 1_000_000

        # Get all premium efficiency ratios
        all_efficiency_ratios = []
        for q in all_quotes:
            q_liability = getattr(q, 'liability_structure', None)
            q_limit = 0

            if q_liability and hasattr(q_liability, 'per_claim_limit') and q_liability.per_claim_limit:
                q_limit = float(q_liability.per_claim_limit)
            elif q.sum_insured_total:
                q_limit = float(q.sum_insured_total)

            q_premium = _normalize_premium(q.premium_amount) or 0

            if q_limit > 0 and q_premium > 0:
                all_efficiency_ratios.append((q_premium / q_limit) * 1_000_000)

        if all_efficiency_ratios and min(all_efficiency_ratios) > 0:
            # Lower premium per million = better efficiency = higher score
            min_ratio = min(all_efficiency_ratios)

            # Score inversely proportional to premium per million
            efficiency_score = 100 - min(100, ((premium_per_million - min_ratio) / min_ratio * 100))
            efficiency_score = max(0, efficiency_score)
        else:
            efficiency_score = 50

        logger.debug(f"  Premium Efficiency: {efficiency_score:.1f} (SAR {premium_per_million:,.0f} per million)")
    else:
        efficiency_score = 30  # Invalid data

    score_breakdown['premium_efficiency'] = efficiency_score * weights['premium_efficiency']

    # COMPONENT 6: RATE (10%)
    # Same logic as property, but lower weight
    rate_value = _extract_rate_value(quote.rate)
    all_rates = [_extract_rate_value(q.rate) for q in all_quotes if _extract_rate_value(q.rate) > 0]

    if all_rates and max(all_rates) > min(all_rates):
        # Lower rate = better
        rate_score = 100 - ((rate_value - min(all_rates)) / (max(all_rates) - min(all_rates)) * 100)
        rate_score = max(0, min(100, rate_score))
    else:
        rate_score = 50

    score_breakdown['rate'] = rate_score * weights['rate']

    # COMPONENT 7: EXCLUSIONS (5%)
    # Same as property
    exclusions_count = len(quote.exclusions or [])
    all_exclusions_counts = [len(q.exclusions or []) for q in all_quotes]

    if all_exclusions_counts and max(all_exclusions_counts) > 0:
        exclusions_score = 100 - ((exclusions_count / max(all_exclusions_counts)) * 100)
    else:
        exclusions_score = 50

    score_breakdown['exclusions'] = exclusions_score * weights['exclusions']

    # COMPONENT 8: WARRANTIES (4%)
    # Same as property
    warranties_count = len(quote.warranties or [])
    all_warranties_counts = [len(q.warranties or []) for q in all_quotes]

    if all_warranties_counts and max(all_warranties_counts) > 0:
        warranties_score = 100 - ((warranties_count / max(all_warranties_counts)) * 100)
    else:
        warranties_score = 50

    score_breakdown['warranties'] = warranties_score * weights['warranties']

    # COMPONENT 9: HAKIM SCORE (30%)
    # Same as property
    ia_compliant = getattr(quote, 'ia_compliant', False)
    hakim_score, hakim_tier, hakim_rank = _get_hakim_score(quote.company_name, ia_compliant)

    score_breakdown['provider_reputation'] = hakim_score * weights['provider_reputation']
    score_breakdown['hakim_score'] = hakim_score
    score_breakdown['hakim_tier'] = hakim_tier
    score_breakdown['hakim_rank'] = hakim_rank

    # TOTAL SCORE
    total_score = sum([
        score_breakdown.get('per_claim_coverage', 0),
        score_breakdown.get('aggregate_coverage', 0),
        score_breakdown.get('defense_costs_bonus', 0),
        score_breakdown.get('coverage_breadth', 0),
        score_breakdown.get('premium_efficiency', 0),
        score_breakdown.get('rate', 0),
        score_breakdown.get('exclusions', 0),
        score_breakdown.get('warranties', 0),
        score_breakdown.get('provider_reputation', 0),
    ])

    logger.info(f"LIABILITY SCORE for {quote.company_name}: {total_score:.2f}/100")
    logger.debug(f"  Breakdown: Coverage={score_breakdown.get('per_claim_coverage', 0):.1f}, "
                 f"Efficiency={score_breakdown.get('premium_efficiency', 0):.1f}, "
                 f"Defense={score_breakdown.get('defense_costs_bonus', 0):.1f}, "
                 f"Hakim={score_breakdown.get('provider_reputation', 0):.1f}")

    return total_score, score_breakdown


def _calculate_score_by_category(
    quote: ExtractedQuoteData,
    all_quotes: List[ExtractedQuoteData],
    weights: Dict[str, float] = None
) -> Tuple[float, Dict[str, float]]:
    """
    Route to appropriate scoring function based on insurance category.

    Args:
        quote: Quote to score
        all_quotes: All quotes in comparison (for normalization context)
        weights: Optional custom weights

    Returns:
        (total_score, score_breakdown)
    """
    # Detect category
    category = getattr(quote, 'insurance_category', None)

    if not category:
        # Fallback: Try to infer from policy_type
        policy_type = getattr(quote, 'policy_type', '') or ''
        policy_lower = policy_type.lower()

        if any(k in policy_lower for k in ['liability', 'cgl', 'third party', 'professional indemnity']):
            category = 'liability'
        else:
            category = 'property'  # Default

    logger.info(f"Scoring {quote.company_name} as {category.upper()} insurance")

    # Route to appropriate scoring function
    if category == 'liability':
        return _calculate_liability_score(quote, all_quotes, weights)
    else:
        # Property and other types use existing weighted score
        # Need to extract all the list parameters from all_quotes
        all_premiums = [_get_normalized_premium_for_comparison(q) for q in all_quotes]
        all_rates = [_extract_rate_value(q.rate) for q in all_quotes]
        all_benefits_counts = [len(q.key_benefits or []) for q in all_quotes]
        all_exclusions_counts = [len(q.exclusions or []) for q in all_quotes]
        all_warranties_counts = [len(q.warranties or []) for q in all_quotes]

        all_extensions_counts = []
        all_subjectivities_counts = []
        for q in all_quotes:
            extended = getattr(q, '_extended_data', {}) or {}
            extensions_data = extended.get('extensions_and_conditions', {})
            extensions_list = extensions_data.get('extensions_list', []) if isinstance(extensions_data, dict) else []
            all_extensions_counts.append(len(extensions_list))

            subjectivities = _extract_subjectivities(q)
            all_subjectivities_counts.append(len(subjectivities))

        if weights is None:
            weights = DEFAULT_WEIGHTS.copy()

        return _calculate_weighted_score(
            quote, all_premiums, all_rates, all_benefits_counts, all_exclusions_counts,
            all_warranties_counts, all_extensions_counts, all_subjectivities_counts, weights
        )


async def _get_ai_ranking_analysis(
    quote_data_for_ai: List[Dict],
    weighted_scores: List[Tuple]
) -> Dict:
    """
    Use AI to analyze and provide intelligent ranking insights.
    Returns AI-powered analysis including recommendations and reasoning.
    """
    try:
        # Prepare summary for AI
        quotes_summary = []
        for quote_data in quote_data_for_ai:
            quotes_summary.append({
                "company": quote_data.get('company', 'Unknown'),
                "premium": quote_data.get('premium', 0),
                "rate": quote_data.get('rate', 'N/A'),
                "hakim_score": quote_data.get('hakim_score', 0),
                "hakim_tier": quote_data.get('hakim_tier', 'Unknown'),
                "benefits_count": quote_data.get('benefits_count', 0),
                "exclusions_count": quote_data.get('exclusions_count', 0),
                "coverage_limit": quote_data.get('coverage_limit', 'N/A'),
                "score": quote_data.get('weighted_score', 0)
            })
        
        # Find the ranked order
        ranked_companies = [item[0].company_name for item in weighted_scores]
        
        prompt = f"""You are an expert insurance advisor analyzing {len(quotes_summary)} insurance quotes.

Here are the quotes with their scores:
{json.dumps(quotes_summary, indent=2)}

Current ranking (by weighted score): {', '.join(ranked_companies)}

Analyze these quotes and provide:
1. **Best Overall Recommendation**: Which company offers the best overall value considering premium, rate, reputation (Hakim Score), and coverage?
2. **Best Value for Money**: Which offers the best balance of price and quality?
3. **Most Comprehensive Coverage**: Which has the best coverage features?
4. **Key Insights**: 2-3 important insights about these quotes
5. **Risk Considerations**: Any important risks or considerations for each option

Consider:
- Premium and Rate (most important factors - 60% combined weight)
- Hakim Score (reputation & financial stability - 15% weight)
- Coverage quality and benefits
- Exclusions and limitations

Return a JSON response with this structure:
{{
  "best_overall": {{
    "company": "company name",
    "reasoning": "why this is the best overall choice"
  }},
  "best_value": {{
    "company": "company name",
    "reasoning": "why this offers best value"
  }},
  "best_coverage": {{
    "company": "company name",
    "reasoning": "why this has best coverage"
  }},
  "key_insights": [
    "insight 1",
    "insight 2",
    "insight 3"
  ],
  "risk_considerations": {{
    "company_name": "risk or consideration for this company"
  }},
  "ranking_validation": {{
    "is_ranking_appropriate": true/false,
    "suggested_adjustments": "any suggested ranking adjustments"
  }}
}}"""

        response = await openai_client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are an expert insurance advisor providing intelligent analysis of insurance quotes. Always consider premium, rate, reputation, and coverage quality."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1500,
            response_format={"type": "json_object"}
        )
        
        ai_response = json.loads(response.choices[0].message.content)
        
        return {
            "enabled": True,
            "analysis": ai_response,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ AI ranking analysis error: {e}")
        return {
            "enabled": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


def _assign_correct_badge(rank: int, total_quotes: int, premium: float, all_premiums: List[float]) -> str:
    """Assign badges based on actual value logic."""
    if rank == 1:
        return "Recommended"
    elif rank == 2:
        return "Good Option"
    else:
        if premium == max(all_premiums):
            return "Higher Cost Option"
        else:
            return "Consider Alternatives"


# ==============================================================================
# CRITICAL FIX: Unique warranties/subjectivities detection using semantic matching
# Version: 7.1 - Enhanced with similarity-based comparison
# ==============================================================================

def _get_extended_data(quote: ExtractedQuoteData) -> Dict:
    """
    Safely extract _extended_data from a Pydantic model.
    Handles cases where _extended_data is stored as extra field.
    """
    extended = {}
    
    # Method 1: Try getattr (for when it's set as an attribute)
    if hasattr(quote, '_extended_data'):
        extended = getattr(quote, '_extended_data', {}) or {}
        if extended:
            logger.debug(f"   Found _extended_data via getattr")
            return extended
    
    # Method 2: Try model_dump() (Pydantic v2) - include all fields
    if not extended:
        try:
            if hasattr(quote, 'model_dump'):
                # Try with include/exclude to get all fields
                quote_dict = quote.model_dump(exclude_none=False, exclude_unset=False)
                extended = quote_dict.get('_extended_data', {}) or {}
                if extended:
                    logger.debug(f"   Found _extended_data via model_dump()")
                    return extended
        except Exception as e:
            logger.debug(f"   model_dump() failed: {e}")
    
    # Method 3: Try dict() (Pydantic v1) - include all fields
    if not extended:
        try:
            if hasattr(quote, 'dict'):
                quote_dict = quote.dict(exclude_none=False, exclude_unset=False)
                extended = quote_dict.get('_extended_data', {}) or {}
                if extended:
                    logger.debug(f"   Found _extended_data via dict()")
                    return extended
        except Exception as e:
            logger.debug(f"   dict() failed: {e}")
    
    # Method 4: Try __dict__ directly
    if not extended and hasattr(quote, '__dict__'):
        extended = quote.__dict__.get('_extended_data', {}) or {}
        if extended:
            logger.debug(f"   Found _extended_data via __dict__")
            return extended
    
    # Method 5: Try accessing via pydantic extra fields
    if not extended:
        try:
            if hasattr(quote, '__pydantic_extra__'):
                extended = quote.__pydantic_extra__.get('_extended_data', {}) or {}
                if extended:
                    logger.debug(f"   Found _extended_data via __pydantic_extra__")
                    return extended
        except Exception as e:
            logger.debug(f"   __pydantic_extra__ failed: {e}")
    
    # Method 6: Try model_fields_set or model_extra
    if not extended:
        try:
            if hasattr(quote, 'model_extra') and quote.model_extra:
                extended = quote.model_extra.get('_extended_data', {}) or {}
                if extended:
                    logger.debug(f"   Found _extended_data via model_extra")
                    return extended
        except Exception as e:
            logger.debug(f"   model_extra failed: {e}")
    
    # Method 7: Try to access via __pydantic_fields__ or check all attributes
    if not extended:
        try:
            # Check all attributes of the quote object
            for attr_name in dir(quote):
                if attr_name == '_extended_data' or attr_name.endswith('_extended_data'):
                    attr_value = getattr(quote, attr_name, None)
                    if isinstance(attr_value, dict) and attr_value:
                        extended = attr_value
                        logger.debug(f"   Found _extended_data via dir() as '{attr_name}'")
                        return extended
        except Exception as e:
            logger.debug(f"   dir() scan failed: {e}")
    
    # Debug: Log what we actually have if still not found
    if not extended:
        logger.warning(f"   ⚠️ Could not find _extended_data. Quote type: {type(quote)}")
        # Try to see what's actually in the quote
        try:
            if hasattr(quote, 'model_dump'):
                all_data = quote.model_dump(exclude_none=False, exclude_unset=False)
                logger.warning(f"   All quote data keys: {list(all_data.keys())[:30]}")
                if '_extended_data' in all_data:
                    logger.warning(f"   ⚠️ _extended_data found in model_dump but was empty!")
        except:
            pass
    
    return extended if extended else {}

def _normalize_warranty_text(text: str) -> str:
    """
    Normalize warranty text for semantic comparison.
    Removes numbers, punctuation, and standardizes format.
    """
    if not text:
        return ""
    
    text = text.lower()
    
    # Normalize numbers with units
    text = re.sub(r'\d+\s*(cm|centimeter|centimeters)', '[NUMBER] cm', text)
    text = re.sub(r'\d+\s*(inch|inches)', '[NUMBER] inch', text)
    text = re.sub(r'\d+\s*(meter|meters|m\b)', '[NUMBER] meter', text)
    text = re.sub(r'\d+\s*(days?|hours?|weeks?|months?)', '[NUMBER] time', text)
    text = re.sub(r'\d+\s*%', '[NUMBER] percent', text)
    
    # Remove parentheses, brackets, and other punctuation
    text = re.sub(r'[\(\)\[\]\{\}]', '', text)
    text = re.sub(r'[,\.\;\:]', ' ', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove common prefixes
    text = re.sub(r'^\s*[\(]?w\d+[\)]?\s*', '', text)  # (W16), W16, etc.
    text = re.sub(r'^\s*warranty\s*:?\s*', '', text)
    text = re.sub(r'^\s*warranted\s+', '', text)
    
    return text.strip()


def _are_items_similar(item1: str, item2: str, threshold: float = 0.80) -> bool:
    """
    Check if two items (warranties/subjectivities/exclusions) are semantically similar.
    Uses multiple comparison strategies to avoid false negatives.
    """
    if not item1 or not item2:
        return False
    
    # Normalize both texts
    norm1 = _normalize_warranty_text(item1)
    norm2 = _normalize_warranty_text(item2)
    
    if not norm1 or not norm2:
        return False
    
    # Strategy 1: Exact match after normalization
    if norm1 == norm2:
        return True
    
    # Strategy 2: One contains the other
    if norm1 in norm2 or norm2 in norm1:
        return True
    
    # Strategy 3: Character-level similarity (Levenshtein-like)
    similarity = SequenceMatcher(None, norm1, norm2).ratio()
    if similarity >= threshold:
        return True
    
    # Strategy 4: Key phrases matching
    key_phrases = {
        'sprinkler': ['sprinkler', 'sprinklers'],
        'security': ['security', 'guard', 'cctv', 'camera', 'surveillance'],
        'fire': ['fire extinguish', 'fire fight', 'fire equipment', 'fire appliance'],
        'smoking': ['no smoking', 'smoking', 'smoke'],
        'hot_work': ['hot work', 'hotwork', 'welding', 'cutting', 'grinding'],
        'housekeeping': ['housekeeping', 'house keeping', 'cleanliness', 'clean'],
        'hazardous': ['hazardous', 'dangerous', 'chemical'],
        'civil_defense': ['civil defense', 'civil defence', 'cd certificate'],
        'stillage': ['stillage', 'pallets', 'raised', 'elevated', 'off ground', 'above ground'],
        'bookkeeping': ['bookkeeping', 'book keeping', 'accounting', 'records'],
        'premium_payment': ['premium payment', 'payment', '100%', 'inception'],
        'testing': ['testing', 'commissioning', 'test', 'commission'],
        'pump': ['pump', 'fire pump', 'diesel pump', 'water pump'],
        'gps': ['gps', 'coordinates', 'location', 'longitude', 'latitude'],
        'photos': ['photo', 'picture', 'image', 'visual', 'photograph'],
        'valuation': ['valuation', 'appraisal', 'assessment report'],
        'survey': ['survey', 'inspection', 'risk assessment'],
        'kyc': ['kyc', 'aml', 'know your customer'],
        'sama': ['sama', 'ia', 'insurance authority', 'circular'],
        'business_plan': ['business contingency', 'continuity plan', 'disaster recovery']
    }
    
    # Find which key phrase categories match
    item1_categories = set()
    item2_categories = set()
    
    for category, phrases in key_phrases.items():
        if any(phrase in norm1 for phrase in phrases):
            item1_categories.add(category)
        if any(phrase in norm2 for phrase in phrases):
            item2_categories.add(category)
    
    # If they share key phrase categories, they're likely the same
    if item1_categories and item2_categories:
        overlap = item1_categories & item2_categories
        if overlap:
            return True
    
    return False


def _find_unique_items_semantic(provider_items: List[str], all_other_items: List[str]) -> List[str]:
    """
    Find items that are truly unique to this provider using semantic comparison.
    
    Args:
        provider_items: Items from one provider
        all_other_items: Combined items from all other providers
        
    Returns:
        List of unique items
    """
    if not provider_items:
        return []
    
    if not all_other_items:
        return provider_items[:10]  # If no other items, all are unique
    
    unique_items = []
    
    for item in provider_items:
        is_unique = True
        
        for other_item in all_other_items:
            if _are_items_similar(item, other_item):
                is_unique = False
                break
        
        if is_unique:
            unique_items.append(item)
    
    return unique_items[:10]  # Limit to top 10


def _extract_subjectivities_full(quote: ExtractedQuoteData) -> List[str]:
    """
    Extract ALL subjectivities from a quote (comprehensive).
    Extracts from all possible locations in the quote data structure.
    ENHANCED VERSION: More comprehensive extraction with debugging.
    """
    all_subjectivities = []
    
    # Get extended data - Pydantic models store extra fields in model_dump() or __dict__
    # Try multiple methods to access _extended_data
    extended = {}
    
    # Method 1: Try getattr (for when it's set as an attribute)
    if hasattr(quote, '_extended_data'):
        extended = getattr(quote, '_extended_data', {}) or {}
    
    # Method 2: Try model_dump() (Pydantic v2) or dict() (Pydantic v1)
    if not extended:
        try:
            # Try Pydantic v2 method
            if hasattr(quote, 'model_dump'):
                quote_dict = quote.model_dump()
                extended = quote_dict.get('_extended_data', {}) or {}
            # Try Pydantic v1 method
            elif hasattr(quote, 'dict'):
                quote_dict = quote.dict()
                extended = quote_dict.get('_extended_data', {}) or {}
        except Exception as e:
            logger.debug(f"   Could not use model_dump/dict: {e}")
    
    # Method 3: Try __dict__ directly
    if not extended and hasattr(quote, '__dict__'):
        extended = quote.__dict__.get('_extended_data', {}) or {}
    
    # Method 4: Try accessing via model_fields_set or extra fields
    if not extended:
        try:
            # Check if it's in the model's extra fields
            if hasattr(quote, '__pydantic_extra__'):
                extended = quote.__pydantic_extra__.get('_extended_data', {}) or {}
        except:
            pass
    
    subjectivities_data = extended.get('subjectivities', {}) if extended else {}
    
    # Debug: Log what we found (using INFO so it shows in terminal)
    logger.info(f"\n{'='*60}")
    logger.info(f"🔍 EXTRACTING SUBJECTIVITIES for: {quote.company_name}")
    logger.info(f"{'='*60}")
    logger.info(f"   Extended data exists: {bool(extended)}")
    logger.info(f"   Extended data keys: {list(extended.keys()) if extended else 'None'}")
    logger.info(f"   Subjectivities data exists: {bool(subjectivities_data)}")
    logger.info(f"   Subjectivities data type: {type(subjectivities_data)}")
    logger.info(f"   Subjectivities data: {subjectivities_data}")
    
    # Check all possible quote attributes
    logger.info(f"   Quote has _extended_data attr: {hasattr(quote, '_extended_data')}")
    logger.info(f"   Quote has subscriptions: {hasattr(quote, 'subscriptions')}")
    logger.info(f"   Quote has subjectivities: {hasattr(quote, 'subjectivities')}")
    logger.info(f"   Quote has additional_info: {hasattr(quote, 'additional_info')}")
    
    # Also print the raw extended data structure for debugging (first 1000 chars)
    if extended:
        try:
            extended_str = json.dumps(extended, indent=2, default=str)
            logger.info(f"   Extended data preview (first 1000 chars):\n{extended_str[:1000]}")
        except Exception as e:
            logger.info(f"   Could not serialize extended_data: {e}")
    
    # Case 1: If subjectivities_data is a dict (expected structure)
    if isinstance(subjectivities_data, dict):
        # Extract from all subjectivity categories
        for key in ['binding_requirements', 'conditions_precedent', 'documentation_required', 
                    'subjectivities_list', 'requirements', 'all_subjectivities', 'subjectivities']:
            items = subjectivities_data.get(key, [])
            if isinstance(items, list):
                logger.info(f"   ✅ Found {len(items)} items in key '{key}': {items[:3]}")
                all_subjectivities.extend(items)
            elif isinstance(items, str):
                # Sometimes items might be a single string
                all_subjectivities.append(items)
    
    # Case 2: If subjectivities_data is directly a list
    elif isinstance(subjectivities_data, list):
        logger.info(f"   ✅ Subjectivities data is a list with {len(subjectivities_data)} items: {subjectivities_data[:3]}")
        all_subjectivities.extend(subjectivities_data)
    
    # Case 3: Check if subjectivities are stored at the root level of extended_data
    for alt_key in ['subjectivities_and_requirements', 'binding_requirements', 'conditions_precedent']:
        alt_data = extended.get(alt_key, {})
        if isinstance(alt_data, dict):
            for key in ['binding_requirements', 'conditions_precedent', 'documentation_required']:
                items = alt_data.get(key, [])
                if isinstance(items, list):
                    logger.info(f"   ✅ Found {len(items)} items in extended_data['{alt_key}']['{key}']: {items[:3]}")
                    all_subjectivities.extend(items)
        elif isinstance(alt_data, list):
            logger.info(f"   ✅ Found {len(alt_data)} items in extended_data['{alt_key}']: {alt_data[:3]}")
            all_subjectivities.extend(alt_data)
    
    # Case 4: Check subscriptions field (might be typo for subjectivities)
    if hasattr(quote, 'subscriptions') and quote.subscriptions:
        if isinstance(quote.subscriptions, list):
            logger.info(f"   ✅ Found {len(quote.subscriptions)} items in quote.subscriptions: {quote.subscriptions[:3]}")
            all_subjectivities.extend(quote.subscriptions)
    
    # Case 5: Check if there's a direct subjectivities attribute
    if hasattr(quote, 'subjectivities') and quote.subjectivities:
        if isinstance(quote.subjectivities, list):
            logger.info(f"   ✅ Found {len(quote.subjectivities)} items in quote.subjectivities: {quote.subjectivities[:3]}")
            all_subjectivities.extend(quote.subjectivities)
    
    # Case 6: Check additional_info for subjectivities
    if hasattr(quote, 'additional_info') and quote.additional_info:
        if isinstance(quote.additional_info, dict):
            for key in ['subjectivities', 'subjectivities_and_requirements', 'binding_requirements']:
                data = quote.additional_info.get(key)
                if isinstance(data, list):
                    logger.info(f"   ✅ Found {len(data)} items in additional_info['{key}']: {data[:3]}")
                    all_subjectivities.extend(data)
                elif isinstance(data, dict):
                    for sub_key, sub_value in data.items():
                        if isinstance(sub_value, list):
                            logger.info(f"   ✅ Found {len(sub_value)} items in additional_info['{key}']['{sub_key}']: {sub_value[:3]}")
                            all_subjectivities.extend(sub_value)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_subj = []
    for item in all_subjectivities:
        if item:
            # Convert to string if not already
            item_str = str(item).strip()
            if item_str and item_str not in seen:
                seen.add(item_str)
                unique_subj.append(item_str)
    
    logger.info(f"   📊 Total unique subjectivities extracted: {len(unique_subj)}")
    if unique_subj:
        logger.info(f"   📝 Sample subjectivities: {unique_subj[:3]}")
    else:
        # Fallback: Try the simpler extraction method
        logger.info(f"   ⚠️ No subjectivities found with full extraction, trying fallback method")
        fallback_subj = _extract_subjectivities(quote)
        if fallback_subj:
            logger.info(f"   ✅ Fallback method found {len(fallback_subj)} subjectivities")
            unique_subj = fallback_subj
    
    # Log summary at info level
    if unique_subj:
        logger.info(f"   ✅ Extracted {len(unique_subj)} subjectivities for {quote.company_name}")
    else:
        logger.warning(f"   ⚠️ No subjectivities found for {quote.company_name} after all extraction methods")
    
    return unique_subj


def _identify_unique_items(quotes: List[ExtractedQuoteData]) -> Dict[str, Dict]:
    """
    Identify unique warranties, exclusions, extensions, subjectivities for each provider.
    FIXED VERSION v7.1: Uses semantic matching instead of exact text matching.
    
    Returns:
        Dictionary with structure:
        {
            'Provider Name': {
                'unique_warranties': [...],
                'unique_extensions': [...],
                'unique_exclusions': [...],
                'unique_subjectivities': [...]
            }
        }
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🔍 IDENTIFYING UNIQUE ITEMS using SEMANTIC MATCHING v7.1")
    logger.info(f"{'='*80}")
    
    # Step 1: Collect all items per provider
    all_items = {}
    
    for quote in quotes:
        company = quote.company_name
        extended = _get_extended_data(quote)
        
        # Get warranties
        warranties_actual = extended.get('warranties_actual', {})
        warranties_list = warranties_actual.get('warranties_list', []) if isinstance(warranties_actual, dict) else []
        if not warranties_list:
            warranties_list = quote.warranties or []
        
        # Get extensions
        extensions_data = extended.get('extensions_and_conditions', {})
        extensions_list = extensions_data.get('extensions_list', []) if isinstance(extensions_data, dict) else []
        
        # Get exclusions
        exclusions_data = extended.get('exclusions_complete', {})
        exclusions_list = exclusions_data.get('all_exclusions_list', []) if isinstance(exclusions_data, dict) else []
        if not exclusions_list:
            exclusions_list = quote.exclusions or []
        
        # Get subjectivities (COMPREHENSIVE)
        subjectivities_list = _extract_subjectivities_full(quote)
        
        all_items[company] = {
            'warranties': warranties_list,
            'extensions': extensions_list,
            'exclusions': exclusions_list,
            'subjectivities': subjectivities_list
        }
        
        logger.info(f"\n📊 {company}:")
        logger.info(f"   Warranties: {len(warranties_list)}")
        logger.info(f"   Extensions: {len(extensions_list)}")
        logger.info(f"   Exclusions: {len(exclusions_list)}")
        logger.info(f"   Subjectivities: {len(subjectivities_list)}")
    
    # Step 2: Find unique items for each provider using SEMANTIC MATCHING
    unique_per_provider = {}
    
    for company, items in all_items.items():
        logger.info(f"\n🔎 Analyzing unique items for: {company}")
        
        # Collect ALL items from OTHER providers
        other_warranties = []
        other_extensions = []
        other_exclusions = []
        other_subjectivities = []
        
        for other_company, other_items in all_items.items():
            if other_company != company:
                other_warranties.extend(other_items['warranties'])
                other_extensions.extend(other_items['extensions'])
                other_exclusions.extend(other_items['exclusions'])
                other_subjectivities.extend(other_items['subjectivities'])
        
        # Find unique items using SEMANTIC matching
        unique_warranties = _find_unique_items_semantic(items['warranties'], other_warranties)
        unique_extensions = _find_unique_items_semantic(items['extensions'], other_extensions)
        unique_exclusions = _find_unique_items_semantic(items['exclusions'], other_exclusions)
        unique_subjectivities = _find_unique_items_semantic(items['subjectivities'], other_subjectivities)
        
        unique_per_provider[company] = {
            'unique_warranties': unique_warranties,
            'unique_extensions': unique_extensions,
            'unique_exclusions': unique_exclusions,
            'unique_subjectivities': unique_subjectivities
        }
        
        logger.info(f"   ✓ Unique Warranties: {len(unique_warranties)}")
        logger.info(f"   ✓ Unique Extensions: {len(unique_extensions)}")
        logger.info(f"   ✓ Unique Exclusions: {len(unique_exclusions)}")
        logger.info(f"   ✓ Unique Subjectivities: {len(unique_subjectivities)}")
        
        if unique_warranties:
            logger.info(f"   📝 Sample unique warranties:")
            for w in unique_warranties[:3]:
                logger.info(f"      • {w[:80]}{'...' if len(w) > 80 else ''}")
    
    logger.info(f"\n{'='*80}")
    logger.info(f"✅ UNIQUE ITEMS IDENTIFICATION COMPLETE")
    logger.info(f"{'='*80}\n")
    
    return unique_per_provider


def _extract_subjectivities(quote: ExtractedQuoteData) -> List[str]:
    """Extract subjectivities and binding requirements."""
    extended = _get_extended_data(quote)
    subjectivities = extended.get('subjectivities', {})
    
    if not subjectivities:
        return []
    
    items = []
    
    binding = subjectivities.get('binding_requirements', [])
    if binding:
        items.extend(binding[:5])
    
    conditions = subjectivities.get('conditions_precedent', [])
    if conditions:
        items.extend(conditions[:3])
    
    return items[:8]


def _extract_conditions(quote: ExtractedQuoteData) -> List[str]:
    """Extract conditions from quote."""
    extended = _get_extended_data(quote)
    extensions_data = extended.get('extensions_and_conditions', {})
    
    conditions = []
    
    if isinstance(extensions_data, dict):
        extensions_list = extensions_data.get('extensions_list', [])
        conditions.extend(extensions_list[:10])
    
    if not conditions:
        conditions = [
            "Standard policy terms and conditions apply",
            "Subject to final underwriting approval",
            "Premium payment required at inception"
        ]
    
    return conditions


def _extract_operational_details(quote: ExtractedQuoteData) -> Dict:
    """Extract operational details like validity, payment terms, etc."""
    extended = _get_extended_data(quote)
    operational = extended.get('operational_details', {})
    brokerage = extended.get('brokerage_info', {})
    
    return {
        'validity_period': operational.get('validity_period', 'Not specified'),
        'payment_terms': getattr(quote, 'premium_frequency', 'Not specified'),
        'cancellation_notice': operational.get('cancellation_notice', 'Not specified'),
        'geographical_limits': operational.get('geographical_limits', 'Not specified'),
        'brokerage': brokerage.get('brokerage_percentage', 'Not specified')
    }


async def rank_and_compare_quotes(
    quotes: List[ExtractedQuoteData],
    weights: Optional[Dict[str, float]] = None,
    comparison_id: Optional[str] = None
) -> Dict:
    """
    Comprehensive ranking and comparison engine with Hakim Score.
    Generates complete comparison structure with ALL sections.
    
    Args:
        quotes: List of extracted quotes
        weights: Custom weights dict (uses DEFAULT_WEIGHTS if None)
        comparison_id: Optional comparison ID to use (generates one if None)
    """
    
    if not quotes:
        raise AIRankingError("No quotes provided")
    
    # Filter out rejected quotes (safety check - should not happen if routes.py filters correctly)
    valid_quotes = [
        q for q in quotes 
        if getattr(q, 'quote_status', 'accepted') == 'accepted'
    ]
    
    rejected_count = len(quotes) - len(valid_quotes)
    if rejected_count > 0:
        logger.warning(f"⚠️ Ranker: Filtered out {rejected_count} rejected quote(s)")
    
    if not valid_quotes:
        raise AIRankingError("No valid (accepted) quotes to rank - all quotes were rejected")
    
    if len(valid_quotes) == 1:
        return _create_single_quote_comparison(valid_quotes[0], comparison_id=comparison_id)
    
    # Use valid_quotes for all subsequent processing
    quotes = valid_quotes
    
    if weights is None:
        weights = DEFAULT_WEIGHTS.copy()
    
    # Generate comparison_id if not provided
    if not comparison_id:
        timestamp = int(datetime.now().timestamp())
        unique_id = uuid.uuid4().hex[:12]
        comparison_id = f"cmp_{timestamp}_{unique_id}"
    
    logger.info(f"🔍 Starting comprehensive comparison of {len(quotes)} quotes")
    logger.info(f"⚖️ Using weights: {weights}")
    logger.info(f"🏆 Hakim Score integration enabled")
    
    unique_items = _identify_unique_items(quotes)
    
    all_premiums = [_get_normalized_premium_for_comparison(q) for q in quotes]
    all_rates = [_extract_rate_value(q.rate) for q in quotes]
    all_benefits_counts = [len(q.key_benefits or []) for q in quotes]
    all_exclusions_counts = [len(q.exclusions or []) for q in quotes]
    all_warranties_counts = [len(q.warranties or []) for q in quotes]
    
    # Extract extensions and subjectivities counts
    all_extensions_counts = []
    all_subjectivities_counts = []
    for q in quotes:
        extended = getattr(q, '_extended_data', {}) or {}
        extensions_data = extended.get('extensions_and_conditions', {})
        extensions_list = extensions_data.get('extensions_list', []) if isinstance(extensions_data, dict) else []
        all_extensions_counts.append(len(extensions_list))
        
        subjectivities = _extract_subjectivities(q)
        all_subjectivities_counts.append(len(subjectivities))
    
    quote_data_for_ai = []
    weighted_scores = []
    
    for i, quote in enumerate(quotes, 1):
        extended = _get_extended_data(quote)
        calc_log = getattr(quote, '_calculation_log', {}) or {}
        
        benefits = quote.key_benefits or []
        coverage_details = extended.get('coverage_details', {})
        if isinstance(coverage_details, dict):
            benefits_explained = coverage_details.get('coverage_benefits_explained', [])
            if benefits_explained and len(benefits_explained) > len(benefits):
                benefits = benefits_explained
        
        exclusions = quote.exclusions or []
        exclusions_complete = extended.get('exclusions_complete', {})
        if isinstance(exclusions_complete, dict):
            exclusions_list = exclusions_complete.get('all_exclusions_list', [])
            if exclusions_list and len(exclusions_list) > len(exclusions):
                exclusions = exclusions_list
        
        warranties_actual = extended.get('warranties_actual', {})
        warranties = []
        if isinstance(warranties_actual, dict):
            warranties = warranties_actual.get('warranties_list', [])
        if not warranties:
            warranties = quote.warranties or []
        
        extensions_data = extended.get('extensions_and_conditions', {})
        extensions = []
        if isinstance(extensions_data, dict):
            extensions = extensions_data.get('extensions_list', [])
        
        deductibles_complete = extended.get('deductibles_complete', {})
        deductible_info = quote.deductible or "Not specified"
        if isinstance(deductibles_complete, dict):
            applicable_md = deductibles_complete.get('applicable_md_tier', {})
            applicable_bi = deductibles_complete.get('applicable_bi_tier', {})
            if applicable_md or applicable_bi:
                deductible_info = quote.deductible or calc_log.get('deductible_tier_logic', 'Not specified')
        
        operational = _extract_operational_details(quote)
        subjectivities = _extract_subjectivities(quote)
        
        analysis_details = getattr(quote, '_analysis_details', {}) or {}
        score_breakdown = analysis_details.get('score_breakdown', {})

        # Use category-aware scoring router
        weighted_score, weighted_breakdown = _calculate_score_by_category(
            quote=quote,
            all_quotes=quotes,
            weights=weights
        )
        
        client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
        ia_compliant = getattr(quote, 'ia_compliant', False)
        
        # Use async version for better DB integration
        hakim_score, hakim_tier, hakim_rank = await _get_hakim_score_async(quote.company_name, ia_compliant)
        
        logger.info(f"📊 {quote.company_name}: Hakim Score={hakim_score:.1f} ({hakim_tier}), Weighted Score={weighted_score:.2f}")
        
        quote_summary = {
            'quote_number': i,
            'company': quote.company_name,
            'client_name': client_name,
            'ia_compliant': ia_compliant,
            'hakim_score': hakim_score,
            'hakim_tier': hakim_tier,
            'hakim_rank': hakim_rank,
            'policy_type': quote.policy_type or 'Not specified',
            'premium': quote.premium_amount or 0,
            'rate': quote.rate or 'N/A',
            'annual_cost': quote.total_annual_cost or 0,
            'deductible': deductible_info,
            'coverage_limit': quote.coverage_limit or 'Not specified',
            'score': quote.score or 75,
            'weighted_score': weighted_score,
            'weighted_breakdown': weighted_breakdown,
            'score_breakdown': score_breakdown,
            'benefits_count': len(benefits),
            'benefits': benefits,  # NO LIMIT - show all
            'exclusions_count': len(exclusions),
            'exclusions': exclusions,  # NO LIMIT - show all
            'warranties_count': len(warranties),
            'warranties': warranties,  # NO LIMIT - show all
            'extensions_count': len(extensions),
            'extensions': extensions,  # NO LIMIT - show all
            'unique_warranties': unique_items.get(quote.company_name, {}).get('unique_warranties', []),
            'unique_exclusions': unique_items.get(quote.company_name, {}).get('unique_exclusions', []),
            'unique_subjectivities': unique_items.get(quote.company_name, {}).get('unique_subjectivities', []),
            'strengths': quote.strengths or [],
            'weaknesses': quote.weaknesses or [],
            'operational': operational,
            'subjectivities': subjectivities
        }
        
        quote_data_for_ai.append(quote_summary)
        weighted_scores.append((quote, weighted_score, weighted_breakdown, hakim_score, hakim_tier))
    
    logger.info("🤖 Performing weighted ranking with Hakim Score...")
    
    weighted_scores.sort(key=lambda x: x[1], reverse=True)
    
    ranking_data = {
        'ranking': [],
        'analysis_summary': f"Comparison of {len(quotes)} quotes using weighted scoring with Hakim Score integration",
        'best_overall': weighted_scores[0][0].company_name,
        'best_value': weighted_scores[0][0].company_name,
        'policy_type': quotes[0].policy_type or 'property',
        'weights_used': weights,
        'hakim_score_enabled': True
    }
    
    for rank, (quote, w_score, w_breakdown, h_score, h_tier) in enumerate(weighted_scores, 1):
        matching_summary = next((q for q in quote_data_for_ai if q['company'] == quote.company_name), {})
        
        ranking_data['ranking'].append({
            'rank': rank,
            'company': quote.company_name,
            'client_name': getattr(quote, 'client_name', 'Not specified'),
            'hakim_score': h_score,
            'hakim_tier': h_tier,
            'hakim_rank': w_breakdown.get('hakim_rank', 999),
            'score': w_score,
            'score_reasoning': {
                'premium_score': round(w_breakdown.get('premium', 0), 2),
                'rate_score': round(w_breakdown.get('rate', 0), 2),
                'benefits_score': round(w_breakdown.get('benefits', 0), 2),
                'exclusions_score': round(w_breakdown.get('exclusions', 0), 2),
                'warranties_score': round(w_breakdown.get('warranties', 0), 2),
                'extensions_score': round(w_breakdown.get('extensions', 0), 2),
                'subjectivities_score': round(w_breakdown.get('subjectivities', 0), 2),
                'reputation_score': round(w_breakdown.get('provider_reputation', 0), 2),
                'reputation_based_on': f"Hakim Score: {h_score:.1f}/100 ({h_tier}) - 30% weight"
            },
            'premium': _get_normalized_premium_for_comparison(quote),
            'rate': quote.rate or 'N/A',
            'annual_cost': _normalize_premium(quote.total_annual_cost),
            'reason': f"Weighted score: {w_score:.2f}/100 (Hakim: {h_score:.1f} - {h_tier})",
            'key_advantages': quote.strengths[:3] if quote.strengths else ['Competitive offering'],
            'key_disadvantages': quote.weaknesses[:2] if quote.weaknesses else ['Standard terms'],
            # ✅ FIX: Add ALL benefits, exclusions, warranties to ranking array
            'benefits': quote.key_benefits or [],  # ALL benefits, no limit
            'benefits_count': len(quote.key_benefits) if quote.key_benefits else 0,
            'exclusions': quote.exclusions or [],  # ALL exclusions, no limit
            'exclusions_count': len(quote.exclusions) if quote.exclusions else 0,
            'warranties': quote.warranties or [],  # ALL warranties, no limit
            'warranties_count': len(quote.warranties) if quote.warranties else 0
        })
    
    # PERFORMANCE FIX: Run AI ranking analysis in background task (non-blocking)
    logger.info("🤖 Starting AI ranking analysis (background task)...")
    
    async def run_ai_ranking():
        try:
            result = await _get_ai_ranking_analysis(quote_data_for_ai, weighted_scores)
            logger.info("✅ AI ranking analysis completed")
            return result
        except Exception as e:
            logger.warning(f"⚠️ AI ranking analysis failed: {e}")
            return {"enabled": False, "error": str(e)}
    
    ai_ranking_task = asyncio.create_task(run_ai_ranking())
    
    for item in ranking_data.get('ranking', []):
        rank = item['rank']
        premium = item.get('premium', 0)
        
        item['recommendation_badge'] = _assign_correct_badge(
            rank=rank,
            total_quotes=len(quotes),
            premium=premium,
            all_premiums=all_premiums
        )
    
    logger.info("✅ Weighted ranking with Hakim Score completed")
    
    # PERFORMANCE FIX: Wait for AI ranking analysis to complete before building comparison
    # This runs in parallel with the badge assignment above
    ranking_data['ai_analysis'] = await ai_ranking_task
    
    logger.info("🔨 Building comprehensive comparison structure...")
    
    comparison_response = await _build_comprehensive_comparison(
        quotes=quotes,
        ranking_data=ranking_data,
        unique_items=unique_items,
        quote_data_for_ai=quote_data_for_ai,
        comparison_id=comparison_id
    )
    
    logger.info("✅ Comprehensive comparison complete")
    
    return comparison_response


async def _generate_best_provider_reasoning(
    best_value: str,
    ranking: List[Dict],
    quotes: List[ExtractedQuoteData]
) -> str:
    """
    Generate AI-powered detailed reasoning for why a provider is the best choice.
    """
    try:
        # Find the best provider's ranking info
        best_provider = next((r for r in ranking if r['company'] == best_value), None)
        if not best_provider:
            return f"{best_value} offers the best overall value based on comprehensive analysis."
        
        # Get top 3 competitors for comparison
        competitors = [r for r in ranking[:4] if r['company'] != best_value][:3]
        
        # Build context for AI
        prompt = f"""Analyze why {best_value} is the best recommended insurance provider based on this data:

BEST PROVIDER ({best_value}):
- Rank: #{best_provider['rank']} out of {len(quotes)}
- Score: {best_provider['score']:.1f}/100
- Premium: SAR {best_provider['premium']:,.2f}
- Rate: {best_provider['rate']}
- Hakim Score: {best_provider['hakim_score']:.1f}/100 ({best_provider['hakim_tier']})
- Hakim Rank: #{best_provider['hakim_rank']} in Saudi market
- Key Advantages: {', '.join(best_provider.get('key_advantages', [])[:3])}

TOP COMPETITORS:"""

        for comp in competitors:
            prompt += f"""
- {comp['company']}: Rank #{comp['rank']}, Score {comp['score']:.1f}, Premium SAR {comp['premium']:,.2f}, Hakim: {comp['hakim_score']:.1f}/100"""

        prompt += f"""

Provide a clear, professional 2-3 sentence explanation of WHY {best_value} is the best choice. Focus on:
1. Their competitive advantages (price, reputation, coverage)
2. Why they outperform competitors
3. What makes them the best value

Be specific with numbers and factual. Write in a professional, decisive tone."""

        response = await openai_client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are an expert insurance analyst providing clear, factual recommendations."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=200
        )
        
        reasoning = response.choices[0].message.content.strip()
        logger.info(f"✅ Generated reasoning for {best_value}: {reasoning[:100]}...")
        return reasoning
        
    except Exception as e:
        logger.error(f"❌ Failed to generate reasoning: {e}")
        # Fallback reasoning with proper data handling
        try:
            return f"{best_value} is recommended as the best provider based on optimal balance of price (SAR {best_provider['premium']:,.2f}), overall score ({best_provider['score']:.1f}/100), and market reputation (Hakim Score: {best_provider['hakim_score']:.1f}/100)."
        except:
            return f"{best_value} offers the best overall value based on comprehensive analysis of price, coverage, and reputation."


async def _build_comprehensive_comparison(
    quotes: List[ExtractedQuoteData],
    ranking_data: Dict,
    unique_items: Dict,
    quote_data_for_ai: List[Dict],
    comparison_id: str
) -> Dict:
    """Build the complete comparison structure with all sections."""
    
    ranking = []
    for ranked_item in ranking_data.get('ranking', []):
        company = ranked_item['company']
        
        matching_quote = next((q for q in quotes if q.company_name == company), None)
        if not matching_quote:
            continue
        
        unique = unique_items.get(company, {})
        
        ranking.append({
            'rank': ranked_item['rank'],
            'company': company,
            'client_name': ranked_item.get('client_name', 'Not specified'),
            'hakim_score': ranked_item.get('hakim_score', 0),
            'hakim_tier': ranked_item.get('hakim_tier', 'Standard'),
            'hakim_rank': ranked_item.get('hakim_rank', 999),
            'score': ranked_item['score'],
            'recommendation_badge': ranked_item['recommendation_badge'],
            'premium': _normalize_premium(matching_quote.premium_amount),
            'rate': matching_quote.rate or 'N/A',
            'annual_cost': _normalize_premium(matching_quote.total_annual_cost),
            'reason': ranked_item['reason'],
            'key_advantages': ranked_item.get('key_advantages', []),
            'key_disadvantages': ranked_item.get('key_disadvantages', []),
            'unique_warranties': unique.get('unique_warranties', []),
            'unique_exclusions': unique.get('unique_exclusions', []),
            'unique_subjectivities': unique.get('unique_subjectivities', [])
        })
    
    summary = {
        'ranking': ranking,
        'analysis_summary': ranking_data.get('analysis_summary', ''),
        'best_overall': ranking_data.get('best_overall', ''),
        'best_value': ranking_data.get('best_value', ''),
        'policy_type': ranking_data.get('policy_type', 'property'),
        'weights_used': ranking_data.get('weights_used', DEFAULT_WEIGHTS),
        'hakim_score_enabled': True
    }
    
    # Generate AI-powered recommendation reasoning
    best_provider_reasoning = await _generate_best_provider_reasoning(
        best_value=summary['best_value'],
        ranking=ranking,
        quotes=quotes
    )
    
    key_differences = {
        'differences': [],
        'summary': f"Comprehensive comparison of {len(quotes)} providers with Hakim Score",
        'recommendation': f"{summary['best_value']} offers the best overall value",
        'recommendation_reasoning': best_provider_reasoning  # NEW: Detailed reasoning
    }
    
    for i, quote1 in enumerate(quotes):
        for quote2 in quotes[i+1:]:
            premium1 = _get_normalized_premium_for_comparison(quote1)
            premium2 = _get_normalized_premium_for_comparison(quote2)
            
            if premium1 > 0 and premium2 > 0:
                diff_pct = abs(premium1 - premium2) / max(premium1, premium2) * 100
                
                key_differences['differences'].append({
                    'provider1': quote1.company_name,
                    'provider2': quote2.company_name,
                    'price_difference': abs(premium1 - premium2),
                    'price_difference_percentage': round(diff_pct, 2),
                    'cheaper': quote1.company_name if premium1 < premium2 else quote2.company_name
                })
    
    providers = []
    for quote in quotes:
        client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
        ia_compliant = getattr(quote, 'ia_compliant', False)
        hakim_score, hakim_tier, hakim_rank = await _get_hakim_score_async(quote.company_name, ia_compliant)
        
        # Get extended data
        extended = _get_extended_data(quote)
        subjectivities = _extract_subjectivities(quote)
        extensions_data = extended.get('extensions_and_conditions', {})
        extensions_list = extensions_data.get('extensions_list', []) if isinstance(extensions_data, dict) else []
        
        # Get unique items for this provider
        unique = unique_items.get(quote.company_name, {})
        
        # Build comprehensive provider data with ALL details
        provider_data = {
            'name': quote.company_name,
            'client_name': client_name,
            'ia_compliant': ia_compliant,
            'hakim_score': hakim_score,
            'hakim_tier': hakim_tier,
            'hakim_rank': hakim_rank,
            'rank': next((r['rank'] for r in ranking if r['company'] == quote.company_name), 0),
            'score': quote.score if quote.score else 0,
            'premium': _get_normalized_premium_for_comparison(quote),
            'rate': quote.rate or 'N/A',
            'coverage_limit': quote.coverage_limit or 'N/A',
            'deductible': quote.deductible or 'N/A',
            
            # ALL WARRANTIES
            'warranties': quote.warranties or [],
            'warranties_count': len(quote.warranties) if quote.warranties else 0,
            'unique_warranties': unique.get('unique_warranties', []),
            
            # ALL EXCLUSIONS
            'exclusions': quote.exclusions or [],
            'exclusions_count': len(quote.exclusions) if quote.exclusions else 0,
            'unique_exclusions': unique.get('unique_exclusions', []),
            
            # ALL SUBJECTIVITIES
            'subjectivities': subjectivities,
            'subjectivities_count': len(subjectivities),
            'unique_subjectivities': unique.get('unique_subjectivities', []),
            
            # ALL BENEFITS
            'benefits': quote.key_benefits or [],
            'benefits_count': len(quote.key_benefits) if quote.key_benefits else 0,
            
            # EXTENSIONS
            'extensions': extensions_list,
            'extensions_count': len(extensions_list),
            
            # CONDITIONS
            'conditions': _extract_conditions(quote),
            
            # STRENGTHS AND WEAKNESSES
            'strengths': quote.strengths or [],
            'weaknesses': quote.weaknesses or []
        }
        
        providers.append(provider_data)
    
    comparison_matrix = {
        "premium": [
            {
                "provider": quote.company_name,
                "value": _get_normalized_premium_for_comparison(quote),
                "formatted": f"SAR {_get_normalized_premium_for_comparison(quote):,.2f}"
            }
            for quote in quotes
        ],
        "rate": [
            {
                "provider": quote.company_name,
                "value": quote.rate or "N/A"
            }
            for quote in quotes
        ],
        "score": [
            {
                "provider": quote.company_name,
                "value": quote.score or 0,
                "formatted": f"{quote.score:.2f}" if quote.score else "0.00"
            }
            for quote in quotes
        ],
        "hakim_score": [
            {
                "provider": quote.company_name,
                # Note: This is in a sync context, so we use sync version
                "score": _get_hakim_score(quote.company_name, getattr(quote, 'ia_compliant', False))[0],
                "tier": _get_hakim_score(quote.company_name, getattr(quote, 'ia_compliant', False))[1],
                "rank": _get_hakim_score(quote.company_name, getattr(quote, 'ia_compliant', False))[2]
            }
            for quote in quotes
        ],
        "deductible": [
            {
                "provider": quote.company_name,
                "value": quote.deductible or "N/A"
            }
            for quote in quotes
        ],
        "coverage": [
            {
                "provider": quote.company_name,
                "value": quote.coverage_limit or "N/A"
            }
            for quote in quotes
        ],
        "benefits": [
            {
                "provider": quote.company_name,
                "items": quote.key_benefits
            }
            for quote in quotes
        ],
        "exclusions": [
            {
                "provider": quote.company_name,
                "items": quote.exclusions
            }
            for quote in quotes
        ],
        "warranties": [
            {
                "provider": quote.company_name,
                "items": quote.warranties
            }
            for quote in quotes
        ],
        "subjectivities": [
            {
                "provider": quote.company_name,
                "items": quote.subscriptions
            }
            for quote in quotes
        ],
        "conditions": [
            {
                "provider": quote.company_name,
                "items": _extract_conditions(quote)
            }
            for quote in quotes
        ],
        "benefits_count": [
            {
                "provider": quote.company_name,
                "value": len(quote.key_benefits)
            }
            for quote in quotes
        ],
        "exclusions_count": [
            {
                "provider": quote.company_name,
                "value": len(quote.exclusions)
            }
            for quote in quotes
        ],
        "warranties_count": [
            {
                "provider": quote.company_name,
                "value": len(quote.warranties)
            }
            for quote in quotes
        ]
    }
    
    best_quote = max(quotes, key=lambda q: q.score or 0)
    
    side_by_side = {
        "providers": providers,
        "comparison_matrix": comparison_matrix,
        "summary": f"Detailed side-by-side comparison of {len(quotes)} providers with Hakim Score",
        "winner": best_quote.company_name,
        "winner_reasons": best_quote.strengths[:3] if best_quote.strengths else []
    }
    
    # Build comprehensive data table with ALL fields
    data_table = {
        'columns': ['Provider', 'Score', 'Premium', 'Rate', 'Coverage', 'Benefits', 'Exclusions', 'Warranties', 'Rank'],
        'rows': [],
        'total_rows': len(quotes)
    }
    
    # Get ranking for each quote
    rank_map = {item['company']: item['rank'] for item in ranking}
    
    for quote in quotes:
        client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
        quote_rank = rank_map.get(quote.company_name, 0)
        
        # Get extended data for full details
        extended = _get_extended_data(quote)
        coverage_details = extended.get('coverage_details', {})
        coverage_limit = quote.coverage_limit or coverage_details.get('coverage_limit', 'N/A')
        
        # Build comprehensive row with ALL data
        row = {
            'provider': quote.company_name,
            'provider_name': quote.company_name,  # Alternative key
            'company': quote.company_name,  # Alternative key
            'score': quote.score if quote.score else 0,
            'weighted_score': quote.score if quote.score else 0,
            'premium': _get_normalized_premium_for_comparison(quote),
            'premium_amount': _get_normalized_premium_for_comparison(quote),
            'rate': quote.rate or 'N/A',
            'coverage': coverage_limit if isinstance(coverage_limit, (int, float)) else (coverage_limit if coverage_limit != 'N/A' else 0),
            'coverage_limit': coverage_limit if isinstance(coverage_limit, (int, float)) else (coverage_limit if coverage_limit != 'N/A' else 0),
            'benefits': len(quote.key_benefits) if quote.key_benefits else 0,
            'benefits_count': len(quote.key_benefits) if quote.key_benefits else 0,
            'key_benefits': quote.key_benefits or [],
            'exclusions': len(quote.exclusions) if quote.exclusions else 0,
            'exclusions_count': len(quote.exclusions) if quote.exclusions else 0,
            'exclusions_list': quote.exclusions or [],
            'warranties': len(quote.warranties) if quote.warranties else 0,
            'warranties_count': len(quote.warranties) if quote.warranties else 0,
            'warranties_list': quote.warranties or [],
            'rank': quote_rank,
            'ranking': quote_rank,
            'deductible': quote.deductible or 'N/A',
            'client': client_name,
            'client_name': client_name
        }
        
        # Add subjectivities if available
        subjectivities = _extract_subjectivities(quote)
        if subjectivities:
            row['subjectivities'] = len(subjectivities)
            row['subjectivities_count'] = len(subjectivities)
            row['subjectivities_list'] = subjectivities
        
        # Add extensions if available
        extensions_data = extended.get('extensions_and_conditions', {})
        if isinstance(extensions_data, dict):
            extensions_list = extensions_data.get('extensions_list', [])
            if extensions_list:
                row['extensions'] = len(extensions_list)
                row['extensions_count'] = len(extensions_list)
                row['extensions_list'] = extensions_list
        
        data_table['rows'].append(row)
    
    premiums = [_get_normalized_premium_for_comparison(q) for q in quotes if q.premium_amount]
    scores = [q.score for q in quotes if q.score]
    
    analytics = {
        'charts': [
            {
                'type': 'bar',
                'title': 'Premium Comparison',
                'data': [
                    {'provider': item['company'], 'premium': item['premium'], 'rank': item['rank']}
                    for item in ranking
                ],
                'x_axis': 'provider',
                'y_axis': 'premium',
                'color_by': 'rank'
            },
            {
                'type': 'horizontal_bar',
                'title': 'Hakim Score by Provider',
                'data': [
                    {
                        'provider': item['company'],
                        'hakim_score': item['hakim_score'],
                        'tier': item['hakim_tier']
                    }
                    for item in ranking
                ],
                'x_axis': 'hakim_score',
                'y_axis': 'provider',
                'max_score': 100
            },
            {
                'type': 'horizontal_bar',
                'title': 'Overall Score Comparison',
                'data': [
                    {'provider': item['company'], 'score': item['score'], 'rank': item['rank']}
                    for item in ranking
                ],
                'x_axis': 'score',
                'y_axis': 'provider',
                'max_score': 100
            },
            {
                'type': 'grouped_bar',
                'title': 'Coverage Features Comparison',
                'data': [
                    {
                        'provider': item['company'],
                        'benefits': next((q for q in quote_data_for_ai if q['company'] == item['company']), {}).get('benefits_count', 0),
                        'exclusions': next((q for q in quote_data_for_ai if q['company'] == item['company']), {}).get('exclusions_count', 0),
                        'warranties': next((q for q in quote_data_for_ai if q['company'] == item['company']), {}).get('warranties_count', 0)
                    }
                    for item in ranking
                ],
                'categories': ['benefits', 'exclusions', 'warranties'],
                'x_axis': 'provider'
            }
        ],
        'statistics': {
            'total_quotes': len(quotes),
            'average_premium': sum(premiums) / len(premiums) if premiums else 0,
            'lowest_premium': min(premiums) if premiums else 0,
            'highest_premium': max(premiums) if premiums else 0,
            'average_score': sum(scores) / len(scores) if scores else 0,
            'best_score': max(scores) if scores else 0,
            'price_range_sar': [min(premiums), max(premiums)] if premiums else [0, 0]
        },
        'insights': [
            f"Best value provider: {summary['best_value']}",
            f"Lowest premium: {min(premiums):,.2f} SAR" if premiums else "N/A",
            f"Average score: {sum(scores)/len(scores):.1f}/100" if scores else "N/A"
        ]
    }
    
    provider_cards = []
    for item in ranking:
        company = item['company']
        matching_quote = next((q for q in quotes if q.company_name == company), None)
        quote_data = next((q for q in quote_data_for_ai if q['company'] == company), {})
        
        provider_cards.append({
            'provider_name': company,
            'client_name': item.get('client_name', 'Not specified'),
            'hakim_score': item.get('hakim_score', 0),
            'hakim_tier': item.get('hakim_tier', 'Standard'),
            'rank': item['rank'],
            'score': item['score'],
            'recommendation_badge': item['recommendation_badge'],
            'premium': f"SAR {item['premium']:,.2f}",
            'rate': item['rate'],
            'annual_cost': f"SAR {item['annual_cost']:,.2f}",
            'key_highlights': item['key_advantages'],
            'considerations': item['key_disadvantages'],
            'coverage_summary': f"{quote_data.get('benefits_count', 0)} benefits, {quote_data.get('exclusions_count', 0)} exclusions",
            'action_button': 'Select Provider' if item['rank'] == 1 else 'View Details'
        })
    
    extracted_quotes = []
    for quote in quotes:
        client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
        hakim_score, hakim_tier, hakim_rank = _get_hakim_score(quote.company_name, getattr(quote, 'ia_compliant', False))
        
        extracted_quotes.append({
            'company_name': quote.company_name,
            'client_name': client_name,
            'hakim_score': hakim_score,
            'hakim_tier': hakim_tier,
            'hakim_rank': hakim_rank,
            'policy_type': quote.policy_type,
            'premium_amount': quote.premium_amount,
            'rate': quote.rate,
            'score': quote.score,
            'key_benefits': quote.key_benefits or [],
            'exclusions': quote.exclusions or [],
            'warranties': quote.warranties or [],
            'file_name': quote.file_name
        })
    
    return {
        'comparison_id': comparison_id,  # Use the passed comparison_id
        'status': 'completed',
        'total_quotes': len(quotes),
        'hakim_score_enabled': True,
        'summary': summary,
        'key_differences': key_differences,
        'side_by_side': side_by_side,
        'data_table': data_table,
        'analytics': analytics,
        'provider_cards': provider_cards,
        'extracted_quotes': extracted_quotes,
        'files_processed': [quote.file_name for quote in quotes if quote.file_name],
        'processing_timestamp': datetime.utcnow().isoformat()
    }


def _create_fallback_ranking(quotes: List[ExtractedQuoteData]) -> Dict:
    """Create fallback ranking if AI fails."""
    sorted_quotes = sorted(quotes, key=lambda q: _get_normalized_premium_for_comparison(q) or float('inf'))
    
    all_premiums = [_get_normalized_premium_for_comparison(q) for q in sorted_quotes]
    
    ranking = []
    for i, quote in enumerate(sorted_quotes, 1):
        premium = _get_normalized_premium_for_comparison(quote)
        hakim_score, hakim_tier, hakim_rank = _get_hakim_score(quote.company_name, getattr(quote, 'ia_compliant', False))
        
        ranking.append({
            'rank': i,
            'company': quote.company_name,
            'client_name': getattr(quote, 'client_name', 'Not specified'),
            'hakim_score': hakim_score,
            'hakim_tier': hakim_tier,
            'hakim_rank': hakim_rank,
            'score': 75.0,
            'recommendation_badge': _assign_correct_badge(i, len(quotes), premium, all_premiums),
            'premium': premium,
            'rate': quote.rate or 'N/A',
            'annual_cost': _normalize_premium(quote.total_annual_cost),
            'reason': f'Ranked by premium (fallback mode) - Hakim: {hakim_score:.1f} ({hakim_tier})',
            'key_advantages': quote.strengths or ['Competitive pricing'],
            'key_disadvantages': quote.weaknesses or ['Standard terms'],
            # ✅ FIX: Add ALL benefits, exclusions, warranties to fallback ranking
            'benefits': quote.key_benefits or [],  # ALL benefits, no limit
            'benefits_count': len(quote.key_benefits) if quote.key_benefits else 0,
            'exclusions': quote.exclusions or [],  # ALL exclusions, no limit
            'exclusions_count': len(quote.exclusions) if quote.exclusions else 0,
            'warranties': quote.warranties or [],  # ALL warranties, no limit
            'warranties_count': len(quote.warranties) if quote.warranties else 0
        })
    
    return {
        'ranking': ranking,
        'analysis_summary': 'Quotes ranked by premium with Hakim Score (AI ranking unavailable)',
        'best_overall': ranking[0]['company'],
        'best_value': ranking[0]['company'],
        'policy_type': 'property',
        'weights_used': DEFAULT_WEIGHTS,
        'hakim_score_enabled': True
    }


def _create_single_quote_comparison(quote: ExtractedQuoteData, comparison_id: Optional[str] = None) -> Dict:
    """Create comparison structure for single quote."""
    
    premium = _get_normalized_premium_for_comparison(quote)
    annual_cost = _normalize_premium(quote.total_annual_cost)
    client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
    hakim_score, hakim_tier, hakim_rank = _get_hakim_score(quote.company_name, getattr(quote, 'ia_compliant', False))
    
    ranking = [{
        'rank': 1,
        'company': quote.company_name,
        'client_name': client_name,
        'hakim_score': hakim_score,
        'hakim_tier': hakim_tier,
        'hakim_rank': hakim_rank,
        'score': quote.score or 85.0,
        'recommendation_badge': 'Only Option',
        'premium': premium,
        'rate': quote.rate or 'N/A',
        'annual_cost': annual_cost,
        'reason': f'Only quote provided - Hakim Score: {hakim_score:.1f} ({hakim_tier})',
        'key_advantages': quote.strengths or ['Quote extracted successfully'],
        'key_disadvantages': ['No comparison available'],
        # ✅ FIX: Add ALL benefits, exclusions, warranties to single quote ranking
        'benefits': quote.key_benefits or [],  # ALL benefits, no limit
        'benefits_count': len(quote.key_benefits) if quote.key_benefits else 0,
        'exclusions': quote.exclusions or [],  # ALL exclusions, no limit
        'exclusions_count': len(quote.exclusions) if quote.exclusions else 0,
        'warranties': quote.warranties or [],  # ALL warranties, no limit
        'warranties_count': len(quote.warranties) if quote.warranties else 0
    }]
    
    # Generate comparison_id if not provided
    if not comparison_id:
        timestamp = int(datetime.now().timestamp())
        unique_id = uuid.uuid4().hex[:12]
        comparison_id = f"cmp_{timestamp}_{unique_id}"
    
    return {
        'comparison_id': comparison_id,
        'status': 'completed',
        'total_quotes': 1,
        'hakim_score_enabled': True,
        'summary': {
            'ranking': ranking,
            'analysis_summary': f'Single quote analyzed with Hakim Score: {hakim_score:.1f} ({hakim_tier}). Upload multiple quotes for comparison.',
            'best_overall': quote.company_name,
            'best_value': quote.company_name,
            'policy_type': quote.policy_type or 'property',
            'weights_used': DEFAULT_WEIGHTS,
            'hakim_score_enabled': True
        },
        'key_differences': {'differences': [], 'summary': 'N/A', 'recommendation': 'N/A'},
        'side_by_side': {'providers': [], 'comparison_matrix': {}, 'summary': 'N/A'},
        'data_table': {'columns': [], 'rows': [], 'total_rows': 1},
        'analytics': {'charts': [], 'statistics': {}, 'insights': []},
        'provider_cards': [],
        'extracted_quotes': [],
        'files_processed': [quote.file_name] if quote.file_name else [],
        'processing_timestamp': datetime.utcnow().isoformat()
    }


# ============================================================================
# SERVICE CLASS
# ============================================================================

class QuoteRanker:
    """Enhanced Quote Ranker v5.0 with Hakim Score Integration"""
    
    async def rank_and_compare_quotes(self, quotes: List[ExtractedQuoteData], weights: Optional[Dict[str, float]] = None, comparison_id: Optional[str] = None) -> Dict:
        """
        Comprehensive ranking and comparison with Hakim Score (MAIN METHOD).
        
        Args:
            quotes: List of extracted quotes
            weights: Optional custom weights. If None, uses DEFAULT_WEIGHTS
            comparison_id: Optional comparison ID to use
        """
        return await rank_and_compare_quotes(quotes, weights, comparison_id)
    
    async def rank_insurance_quotes(self, quotes: List[ExtractedQuoteData], weights: Optional[Dict[str, float]] = None, comparison_id: Optional[str] = None) -> Dict:
        """Alias for rank_and_compare_quotes."""
        return await rank_and_compare_quotes(quotes, weights, comparison_id)
    
    async def rank_all_quotes(self, quotes: List[ExtractedQuoteData], weights: Optional[Dict[str, float]] = None, comparison_id: Optional[str] = None) -> Dict:
        """Alias for rank_and_compare_quotes."""
        return await rank_and_compare_quotes(quotes, weights, comparison_id)


quote_ranker = QuoteRanker()