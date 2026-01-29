"""
Enhanced Pydantic models for insurance quote system.
Designed to match the frontend UI requirements.

This file now imports from scheme.py to avoid duplication.
"""

# Import all models from the consolidated scheme.py
from .scheme import (
    LiabilityLimitStructure,
    ExtractedQuoteData,
    RankedQuote,
    ComparisonResponse,
    SideBySideComparison,
    KeyDifference,
    ProviderCard,
    ErrorResponse,
    InsuranceQuoteData  # Alias for backward compatibility
)

# Re-export for backward compatibility
__all__ = [
    "LiabilityLimitStructure",
    "ExtractedQuoteData",
    "RankedQuote",
    "ComparisonResponse",
    "SideBySideComparison",
    "KeyDifference",
    "ProviderCard",
    "ErrorResponse",
    "InsuranceQuoteData"
]