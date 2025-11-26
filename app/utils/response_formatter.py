"""
Response Formatter
==================
Formats API responses in a clear, organized manner.
"""

from typing import Dict, Any, List
from app.models.scheme import ComparisonResponse, RankedQuote


class ResponseFormatter:
    """Formats comparison responses for better readability"""
    
    @staticmethod
    def format_comparison_response(response: ComparisonResponse) -> Dict[str, Any]:
        """
        Format comparison response in a clear, hierarchical structure.
        
        Args:
            response: ComparisonResponse object
            
        Returns:
            Formatted dictionary optimized for readability
        """
        formatted = {
            "status": "success",
            "comparison_id": response.comparison_id,
            "analysis_info": {
                "total_quotes_analyzed": response.total_quotes,
                "analysis_timestamp": response.analysis_timestamp,
                "ai_model_used": response.ai_model_used
            },
            "executive_summary": {
                "overview": response.comparison_summary,
                "recommendations": {
                    "best_overall_value": response.best_value,
                    "most_comprehensive_coverage": response.best_coverage,
                    "lowest_premium": response.lowest_price
                }
            },
            "ranked_quotes": []
        }
        
        # Format each ranked quote
        for ranked_quote in response.ranking:
            formatted_quote = ResponseFormatter._format_single_quote(ranked_quote)
            formatted["ranked_quotes"].append(formatted_quote)
        
        return formatted
    
    @staticmethod
    def _format_single_quote(quote: RankedQuote) -> Dict[str, Any]:
        """Format a single ranked quote"""
        return {
            "ranking_position": quote.rank,
            "company_information": {
                "name": quote.company_name,
                "website": quote.extracted_data.company_website,
                "logo": quote.extracted_data.logo_base64
            },
            "overall_assessment": {
                "score": f"{quote.score}/100",
                "value_rating": quote.value_rating,
                "coverage_rating": quote.coverage_rating,
                "price_category": quote.price_category
            },
            "detailed_analysis": {
                "why_this_ranking": quote.reason,
                "recommended_for": quote.recommended_for
            },
            "advantages": quote.pros,
            "limitations": quote.cons,
            "policy_details": {
                "policy_type": quote.extracted_data.policy_type,
                "premium": {
                    "amount": quote.extracted_data.premium_amount,
                    "frequency": quote.extracted_data.premium_frequency,
                    "currency": quote.extracted_data.currency
                },
                "coverage": {
                    "sum_insured": quote.extracted_data.sum_insured,
                    "coverage_limit": quote.extracted_data.coverage_limit,
                    "deductible": quote.extracted_data.deductible
                },
                "policy_period": quote.extracted_data.policy_period,
                "location": quote.extracted_data.location
            },
            "coverage_features": {
                "key_benefits": quote.extracted_data.key_benefits,
                "exclusions": quote.extracted_data.exclusions,
                "additional_coverages": quote.extracted_data.additional_coverages,
                "warranty_conditions": quote.extracted_data.warranty_conditions
            },
            "additional_information": quote.extracted_data.additional_info
        }


# Create singleton
response_formatter = ResponseFormatter()
