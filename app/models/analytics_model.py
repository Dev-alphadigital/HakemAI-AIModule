"""
Analytics and chart data models for frontend visualization.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

            
class ChartDataPoint(BaseModel):
    """Single data point for charts."""
    
    label: str = Field(..., description="Label (e.g., provider name)")
    value: float = Field(..., description="Value")
    color: Optional[str] = Field(None, description="Color for chart")
    percentage: Optional[float] = Field(None, description="Percentage if applicable")


class PremiumComparisonChart(BaseModel):
    """Premium comparison chart data."""
    
    chart_type: str = Field(default="bar", description="Chart type")
    title: str = Field(default="Premium Comparison")
    data: List[ChartDataPoint] = Field(..., description="Chart data points")
    average_premium: float = Field(..., description="Average premium across all")
    lowest_premium: float = Field(..., description="Lowest premium")
    highest_premium: float = Field(..., description="Highest premium")


class CoverageAnalysisChart(BaseModel):
    """Coverage vs premium analysis chart."""
    
    chart_type: str = Field(default="scatter", description="Chart type")
    title: str = Field(default="Coverage Vs Premium Analysis")
    data: List[Dict[str, Any]] = Field(..., description="Coverage data points")
    insights: List[str] = Field(..., description="Key insights from analysis")


class DataTableRow(BaseModel):
    """Single row in the data table."""
    
    provider: str
    score: float
    premium: float
    rate: str
    coverage: str
    deductible: Optional[float] = None
    warranties: int = Field(..., description="Number of warranties")
    exclusions: int = Field(..., description="Number of exclusions")
    rank: int


class DataTableResponse(BaseModel):
    """Complete data table response."""
    
    columns: List[str] = Field(
        default=["Provider", "Score", "Premium", "Rate", "Coverage", "Deductible", "Warranties", "Exclusions", "Rank"]
    )
    rows: List[DataTableRow] = Field(..., description="Table rows")
    total_rows: int = Field(..., description="Total number of rows")
    sortable: bool = Field(default=True)
    filterable: bool = Field(default=True)


class AnalyticsDashboard(BaseModel):
    """Complete analytics dashboard data."""
    
    premium_comparison: PremiumComparisonChart
    coverage_analysis: CoverageAnalysisChart
    data_table: DataTableResponse
    
    # Summary Statistics
    total_providers: int
    average_score: float
    average_premium: float
    best_value_provider: str
    highest_rated_provider: str
    
    # Quick Stats
    price_range: Dict[str, float] = Field(..., description="min and max prices")
    score_range: Dict[str, float] = Field(..., description="min and max scores")