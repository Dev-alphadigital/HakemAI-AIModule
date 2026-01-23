"""
Production-ready Pydantic models for insurance quote comparison.
FIXED: Handles complex deductibles, flexible data types, all insurance types.
✅ UPDATED: Added policy_type_compared field to ComparisonResponse
"""

from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field, validator
from datetime import datetime


# ========================================================================
# CORE MODELS
# ========================================================================

class ExtractedQuoteData(BaseModel):
    """
    Structured data extracted from insurance quote PDF.
    PRODUCTION-READY: Handles all data type variations.
    """
    
    # Basic Information
    company_name: str = Field(..., description="Insurance company name")
    policy_type: Optional[str] = Field(None, description="Type of insurance policy")
    policy_number: Optional[str] = Field(None, description="Policy number if available")
    
    # Pricing Information
    premium_amount: Optional[float] = Field(None, description="Premium amount")
    premium_frequency: Optional[str] = Field(None, description="monthly, annual, quarterly")
    rate: Optional[str] = Field(None, description="Rate description (e.g., '0.35‰')")
    total_annual_cost: Optional[float] = Field(None, description="Total annual cost with fees/VAT")
    vat_amount: Optional[float] = Field(None, description="VAT amount (15% of premium + fees)")
    vat_percentage: Optional[float] = Field(None, description="VAT percentage applied (default 15%)")
    premium_includes_vat: Optional[bool] = Field(False, description="Whether original premium included VAT (always False after normalization)")
    
    # Coverage Details
    score: Optional[float] = Field(None, description="Overall quality score 0-100")
    
    # FIXED: Deductible can be string, number, dict, or null
    deductible: Optional[Union[str, float, Dict[str, Any]]] = Field(
        None, 
        description="Deductible (flexible: can be string description, amount, or structured dict)"
    )
    
    coverage_limit: Optional[str] = Field(None, description="Maximum coverage (string display)")
    sum_insured_total: Optional[float] = Field(None, description="Total sum insured (numeric value)")  # CRITICAL FIX
    coverage_percentage: Optional[float] = Field(None, description="Coverage percentage")
    
    # Detailed Features
    key_benefits: List[str] = Field(default_factory=list, description="Key coverage benefits")
    exclusions: List[str] = Field(default_factory=list, description="Policy exclusions")
    warranties: List[str] = Field(default_factory=list, description="Unique warranties")
    subscriptions: List[str] = Field(default_factory=list, description="Subscription details")
    
    # Strengths and Weaknesses
    strengths: List[str] = Field(default_factory=list, description="Policy strengths")
    weaknesses: List[str] = Field(default_factory=list, description="Policy weaknesses")
    
    # Metadata
    file_name: str = Field(..., description="Original PDF filename")
    extraction_confidence: Optional[str] = Field(None, description="high, medium, low")
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Additional Information
    additional_info: Optional[str] = Field(None, description="Other relevant information")
    raw_text_preview: Optional[str] = Field(None, description="First 200 chars of PDF")
    logo_base64: Optional[str] = Field(None, description="Company logo in base64")
    company_website: Optional[str] = Field(None, description="Company website URL")
    
    # Validator to handle deductible format conversion
    @validator('deductible', pre=True)
    @classmethod
    def parse_deductible(cls, v):
        """
        Parse deductible into flexible format.
        Handles: float, string, dict, null
        """
        if v is None:
            return None
        
        # If already a float, return as-is
        if isinstance(v, (int, float)):
            return float(v)
        
        # If dict or string, keep as-is (flexible Union type)
        if isinstance(v, (dict, str)):
            return v
        
        # Try to parse string to float if it's a simple number
        if isinstance(v, str):
            try:
                # Remove currency symbols and commas
                import re
                cleaned = re.sub(r'[SAR$£€¥,\s]', '', v)
                if cleaned and cleaned.replace('.', '').isdigit():
                    return float(cleaned)
            except:
                pass
            # If can't parse, keep as string
            return v
        
        return v
    
    class Config:
        extra = "allow"  # CRITICAL: Allow extra fields like _extended_data, _analysis_details, etc.
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class RankedQuote(BaseModel):
    """A single ranked insurance quote with AI reasoning."""
    
    rank: int = Field(..., description="Ranking position (1 = best)")
    company: str = Field(..., description="Insurance company name")
    score: float = Field(..., description="Overall score out of 100")
    recommendation_badge: Optional[str] = Field(None, description="Recommended, Best Value, etc.")
    
    # Pricing Summary
    premium: float = Field(..., description="Premium amount")
    rate: str = Field(..., description="Rate display")
    annual_cost: float = Field(..., description="Total annual cost")
    
    # Reasoning
    reason: str = Field(..., description="AI explanation for the ranking")
    key_advantages: List[str] = Field(default_factory=list, description="Main advantages")
    key_disadvantages: List[str] = Field(default_factory=list, description="Main disadvantages")
    
    # Original extracted data
    extracted_data: Optional[ExtractedQuoteData] = None


class ComparisonResponse(BaseModel):
    """
    Final API response containing ranked quotes.
    ✅ FIXED: Added policy_type_compared field for v3.0
    """
    
    ranking: List[RankedQuote] = Field(..., description="List of ranked quotes")
    total_quotes_analyzed: int = Field(..., description="Number of quotes compared")
    analysis_summary: Optional[str] = Field(None, description="Overall comparison summary")
    best_overall: Optional[str] = Field(None, description="Best overall provider name")
    best_value: Optional[str] = Field(None, description="Best value provider name")
    
    # ✅ FIX: Added this field for auto-population system
    policy_type_compared: Optional[str] = Field(
        default="property",
        description="Type of policy that was compared (property, liability, medical, motor, etc.)"
    )
    
    processing_time_seconds: Optional[float] = Field(None, description="Processing duration")
    comparison_id: Optional[str] = Field(None, description="MongoDB ID if saved")


# ========================================================================
# UI FEATURE MODELS
# ========================================================================

class SideBySideComparison(BaseModel):
    """Side-by-side comparison of two quotes."""
    
    provider1: ExtractedQuoteData
    provider2: ExtractedQuoteData
    
    # Comparison Highlights
    price_difference: float = Field(..., description="Price difference in dollars")
    price_difference_percentage: float = Field(..., description="Price difference in percent")
    coverage_comparison: str = Field(..., description="Coverage comparison summary")
    winner: str = Field(..., description="Which provider wins overall")
    
    # Key Differences
    unique_to_provider1: List[str] = Field(default_factory=list)
    unique_to_provider2: List[str] = Field(default_factory=list)
    common_features: List[str] = Field(default_factory=list)


class KeyDifference(BaseModel):
    """Individual key difference between providers."""
    
    category: str = Field(..., description="Category (warranty, exclusion, subscription)")
    provider: str = Field(..., description="Provider name")
    description: str = Field(..., description="Difference description")
    advantage: bool = Field(..., description="Is this an advantage or disadvantage")


class ProviderCard(BaseModel):
    """Provider card data for frontend display."""
    
    provider_name: str
    score: float
    premium: float
    rate: str
    rank: int
    recommendation: Optional[str] = None
    is_recommended: bool = False
    badge_color: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error response model."""
    
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthResponse(BaseModel):
    """Health check response model."""
    
    status: str = Field(..., description="Service status")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    version: str = Field(default="3.0.0", description="API version")
    ai_service: Optional[str] = Field(None, description="AI service status")


# ========================================================================
# ALIASES FOR BACKWARD COMPATIBILITY
# ========================================================================

InsuranceQuoteData = ExtractedQuoteData