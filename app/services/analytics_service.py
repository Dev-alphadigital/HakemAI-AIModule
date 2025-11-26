"""
Service for generating analytics and chart data for frontend visualization.
"""

from typing import List
from app.models.quote_model import ExtractedQuoteData, RankedQuote
from app.models.analytics_model import (
    ChartDataPoint, PremiumComparisonChart, CoverageAnalysisChart,
    DataTableRow, DataTableResponse, AnalyticsDashboard
)


class AnalyticsService:
    """Generate analytics data for charts and tables."""
    
    # Color palette for charts
    COLORS = ["#0D9488", "#F59E0B", "#3B82F6", "#8B5CF6", "#EF4444", "#10B981"]
    
    @staticmethod
    def generate_premium_comparison_chart(quotes: List[RankedQuote]) -> PremiumComparisonChart:
        """
        Generate premium comparison bar chart data.
        
        Args:
            quotes: List of ranked quotes
            
        Returns:
            PremiumComparisonChart object
        """
        
        data_points = []
        premiums = []
        
        for idx, quote in enumerate(quotes):
            premium = quote.premium
            premiums.append(premium)
            
            data_points.append(ChartDataPoint(
                label=quote.company,
                value=premium,
                color=AnalyticsService.COLORS[idx % len(AnalyticsService.COLORS)]
            ))
        
        return PremiumComparisonChart(
            data=data_points,
            average_premium=round(sum(premiums) / len(premiums), 2) if premiums else 0,
            lowest_premium=min(premiums) if premiums else 0,
            highest_premium=max(premiums) if premiums else 0
        )
    
    @staticmethod
    def generate_coverage_analysis_chart(quotes: List[RankedQuote]) -> CoverageAnalysisChart:
        """
        Generate coverage vs premium scatter plot data.
        
        Args:
            quotes: List of ranked quotes
            
        Returns:
            CoverageAnalysisChart object
        """
        
        data_points = []
        insights = []
        
        for quote in quotes:
            extracted = quote.extracted_data
            if extracted:
                data_points.append({
                    "provider": quote.company,
                    "premium": quote.premium,
                    "coverage": extracted.coverage_percentage or 0,
                    "score": quote.score,
                    "deductible": extracted.deductible or 0
                })
        
        # Generate insights
        if quotes:
            best_value = min(quotes, key=lambda q: q.premium / max(q.score, 1))
            insights.append(f"{best_value.company} offers the best value with score {best_value.score:.1f} at ${best_value.premium}")
            
            lowest_premium = min(quotes, key=lambda q: q.premium)
            insights.append(f"{lowest_premium.company} has the lowest premium at ${lowest_premium.premium}")
            
            highest_score = max(quotes, key=lambda q: q.score)
            insights.append(f"{highest_score.company} has the highest overall score of {highest_score.score:.1f}")
        
        return CoverageAnalysisChart(
            data=data_points,
            insights=insights
        )
    
    @staticmethod
    def generate_data_table(quotes: List[RankedQuote]) -> DataTableResponse:
        """
        Generate data table for frontend display.
        
        Args:
            quotes: List of ranked quotes
            
        Returns:
            DataTableResponse object
        """
        
        rows = []
        
        for quote in quotes:
            extracted = quote.extracted_data
            if extracted:
                rows.append(DataTableRow(
                    provider=quote.company,
                    score=round(quote.score, 1),
                    premium=quote.premium,
                    rate=quote.rate,
                    coverage=extracted.coverage_limit or "N/A",
                    deductible=extracted.deductible,
                    warranties=len(extracted.warranties),
                    exclusions=len(extracted.exclusions),
                    rank=quote.rank
                ))
        
        return DataTableResponse(
            rows=rows,
            total_rows=len(rows)
        )
    
    @staticmethod
    def generate_complete_dashboard(quotes: List[RankedQuote]) -> AnalyticsDashboard:
        """
        Generate complete analytics dashboard with all charts and tables.
        
        Args:
            quotes: List of ranked quotes
            
        Returns:
            AnalyticsDashboard object with all analytics
        """
        
        # Generate all components
        premium_chart = AnalyticsService.generate_premium_comparison_chart(quotes)
        coverage_chart = AnalyticsService.generate_coverage_analysis_chart(quotes)
        data_table = AnalyticsService.generate_data_table(quotes)
        
        # Calculate summary statistics
        total_providers = len(quotes)
        average_score = sum(q.score for q in quotes) / total_providers if total_providers > 0 else 0
        average_premium = sum(q.premium for q in quotes) / total_providers if total_providers > 0 else 0
        
        # Find best providers
        best_value = min(quotes, key=lambda q: q.premium / max(q.score, 1)) if quotes else None
        highest_rated = max(quotes, key=lambda q: q.score) if quotes else None
        
        # Price and score ranges
        premiums = [q.premium for q in quotes]
        scores = [q.score for q in quotes]
        
        return AnalyticsDashboard(
            premium_comparison=premium_chart,
            coverage_analysis=coverage_chart,
            data_table=data_table,
            total_providers=total_providers,
            average_score=round(average_score, 2),
            average_premium=round(average_premium, 2),
            best_value_provider=best_value.company if best_value else "N/A",
            highest_rated_provider=highest_rated.company if highest_rated else "N/A",
            price_range={
                "min": min(premiums) if premiums else 0,
                "max": max(premiums) if premiums else 0
            },
            score_range={
                "min": min(scores) if scores else 0,
                "max": max(scores) if scores else 0
            }
        )