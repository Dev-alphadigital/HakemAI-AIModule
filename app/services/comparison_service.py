from typing import List, Dict, Optional, Any
from collections import defaultdict
import logging
import re
from difflib import SequenceMatcher
from app.models.quote_model import ExtractedQuoteData, SideBySideComparison, KeyDifference

logger = logging.getLogger(__name__)


class ComparisonService:
    """Generate detailed comparisons between insurance quotes."""
    
    @staticmethod
    def group_quotes_by_policy_type(quotes: List[ExtractedQuoteData]) -> Dict[str, List[ExtractedQuoteData]]:
        """
        Group quotes by policy type for valid comparisons.
        Only quotes of the same type can be compared.
        
        Args:
            quotes: List of all extracted quotes
            
        Returns:
            Dictionary with policy type as key, list of quotes as value
        """
        grouped = defaultdict(list)
        
        for quote in quotes:
            policy_type = quote.policy_type.lower() if quote.policy_type else "unknown"
            
            if any(keyword in policy_type for keyword in ['property', 'fire', 'all risk', 'material damage', 'business interruption', 'par']):
                category = 'property'
            elif any(keyword in policy_type for keyword in ['liability', 'cgl', 'general liability', 'third party', 'public liability']):
                category = 'liability'
            elif any(keyword in policy_type for keyword in ['medical', 'health', 'malpractice', 'professional indemnity']):
                category = 'medical'
            elif any(keyword in policy_type for keyword in ['motor', 'auto', 'vehicle', 'car']):
                category = 'motor'
            elif any(keyword in policy_type for keyword in ['marine', 'cargo', 'hull']):
                category = 'marine'
            elif any(keyword in policy_type for keyword in ['engineering', 'contractors', 'erection']):
                category = 'engineering'
            else:
                category = 'other'
            
            grouped[category].append(quote)
            logger.info(f"📂 Grouped '{quote.company_name}' ({quote.policy_type}) into category: {category}")
        
        return dict(grouped)
    
    @staticmethod
    def validate_comparable_quotes(quote1: ExtractedQuoteData, quote2: ExtractedQuoteData) -> bool:
        """
        Check if two quotes are comparable.
        
        Args:
            quote1: First quote
            quote2: Second quote
            
        Returns:
            True if quotes can be compared, False otherwise
        """
        type1 = (quote1.policy_type or "").lower()
        type2 = (quote2.policy_type or "").lower()
        
        property_keywords = ['property', 'fire', 'all risk', 'material damage', 'business interruption']
        is_property1 = any(k in type1 for k in property_keywords)
        is_property2 = any(k in type2 for k in property_keywords)
        
        liability_keywords = ['liability', 'cgl', 'general liability', 'third party']
        is_liability1 = any(k in type1 for k in liability_keywords)
        is_liability2 = any(k in type2 for k in liability_keywords)
        
        if (is_property1 and is_property2) or (is_liability1 and is_liability2):
            return True
        
        logger.warning(f"⚠️ Cannot compare: '{type1}' vs '{type2}' - different policy types")
        return False
    
    @staticmethod
    def compare_two_quotes(
        quote1: ExtractedQuoteData,
        quote2: ExtractedQuoteData
    ) -> Optional[SideBySideComparison]:
        """
        Generate side-by-side comparison of two quotes.
        
        Args:
            quote1: First quote to compare
            quote2: Second quote to compare
            
        Returns:
            SideBySideComparison object with detailed comparison, or None if not comparable
        """
        
        if not ComparisonService.validate_comparable_quotes(quote1, quote2):
            logger.warning(f"❌ Cannot compare {quote1.company_name} with {quote2.company_name} - different policy types")
            return None
        
        logger.info(f"✅ Comparing {quote1.company_name} vs {quote2.company_name}")
        
        price1 = quote1.premium_amount or 0
        price2 = quote2.premium_amount or 0
        
        price_diff = abs(price1 - price2)
        price_diff_pct = (price_diff / max(price1, price2) * 100) if max(price1, price2) > 0 else 0
        
        coverage_comparison = ComparisonService._compare_coverage(quote1, quote2)
        
        winner = ComparisonService._determine_winner(quote1, quote2)
        
        unique1 = ComparisonService._find_unique_features(quote1, quote2)
        unique2 = ComparisonService._find_unique_features(quote2, quote1)
        common = ComparisonService._find_common_features(quote1, quote2)
        
        return SideBySideComparison(
            provider1=quote1,
            provider2=quote2,
            price_difference=price_diff,
            price_difference_percentage=round(price_diff_pct, 2),
            coverage_comparison=coverage_comparison,
            winner=winner,
            unique_to_provider1=unique1,
            unique_to_provider2=unique2,
            common_features=common
        )
    
    @staticmethod
    def rank_quotes_by_type(quotes: List[ExtractedQuoteData]) -> Dict[str, List[Dict]]:
        """
        Rank quotes within each policy type category.
        NEVER ranks quotes of different types together.
        
        Args:
            quotes: List of all quotes
            
        Returns:
            Dictionary with policy type as key, ranked list of quotes as value
        """
        grouped = ComparisonService.group_quotes_by_policy_type(quotes)
        
        results = {}
        
        for policy_type, quotes_list in grouped.items():
            if len(quotes_list) == 0:
                continue
            
            logger.info(f"📊 Ranking {len(quotes_list)} {policy_type} quotes")
            
            sorted_quotes = sorted(
                quotes_list,
                key=lambda q: (-(q.score or 0), q.premium_amount or float('inf'))
            )
            
            ranked = []
            for idx, quote in enumerate(sorted_quotes, 1):
                if idx == 1 and (quote.score or 0) >= 85:
                    badge = "Recommended"
                elif quote.premium_amount and quote.premium_amount == min(q.premium_amount or float('inf') for q in quotes_list):
                    badge = "Best Value"
                elif (quote.score or 0) >= 80:
                    badge = "Good Option"
                else:
                    badge = "Consider"
                
                client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Unknown')
                ia_compliant = getattr(quote, 'ia_compliant', False)
                company_name_ar = getattr(quote, 'company_name_ar', None)
                
                ranked_quote = {
                    "rank": idx,
                    "company": quote.company_name,
                    "company_ar": company_name_ar,
                    "client_name": client_name,
                    "ia_compliant": ia_compliant,
                    "score": quote.score or 0,
                    "recommendation_badge": badge,
                    "premium": quote.premium_amount or 0,
                    "rate": quote.rate,
                    "annual_cost": quote.total_annual_cost or 0,
                    "policy_type": quote.policy_type,
                    "deductible": quote.deductible or "N/A",
                    "coverage_limit": quote.coverage_limit or "N/A",
                    "benefits_count": len(quote.key_benefits),
                    "exclusions_count": len(quote.exclusions),
                    "warranties_count": len(quote.warranties),
                    "strengths": quote.strengths or [],
                    "weaknesses": quote.weaknesses or []
                }
                
                ranked.append(ranked_quote)
            
            results[policy_type] = ranked
        
        return results
    
    @staticmethod
    def _compare_coverage(quote1: ExtractedQuoteData, quote2: ExtractedQuoteData) -> str:
        """Compare coverage between two quotes."""
        benefits1 = len(quote1.key_benefits)
        benefits2 = len(quote2.key_benefits)
        
        if benefits1 > benefits2:
            return f"{quote1.company_name} offers {benefits1 - benefits2} more benefits"
        elif benefits2 > benefits1:
            return f"{quote2.company_name} offers {benefits2 - benefits1} more benefits"
        else:
            return "Similar coverage levels"
    
    @staticmethod
    def _determine_winner(quote1: ExtractedQuoteData, quote2: ExtractedQuoteData) -> str:
        """Determine which quote is better overall."""
        score1 = quote1.score or 0
        score2 = quote2.score or 0
        
        if score1 > score2:
            return quote1.company_name
        elif score2 > score1:
            return quote2.company_name
        else:
            premium1 = quote1.premium_amount or float('inf')
            premium2 = quote2.premium_amount or float('inf')
            
            if premium1 < premium2:
                return quote1.company_name
            else:
                return quote2.company_name
    
    @staticmethod
    def _find_unique_features(quote1: ExtractedQuoteData, quote2: ExtractedQuoteData) -> List[str]:
        """Find features unique to quote1."""
        benefits1 = set(quote1.key_benefits)
        benefits2 = set(quote2.key_benefits)
        
        unique = list(benefits1 - benefits2)
        
        return unique[:5]
    
    @staticmethod
    def _find_common_features(quote1: ExtractedQuoteData, quote2: ExtractedQuoteData) -> List[str]:
        """Find common features between quotes."""
        benefits1 = set(quote1.key_benefits)
        benefits2 = set(quote2.key_benefits)
        
        common = list(benefits1 & benefits2)
        
        return common[:10]
    
    # ============================================================================
    # NEW: UNIQUE WARRANTIES DETECTION v7.1
    # ============================================================================
    
    @staticmethod
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
    
    @staticmethod
    def _are_warranties_similar(warranty1: str, warranty2: str, threshold: float = 0.80) -> bool:
        """
        Check if two warranties are semantically similar.
        Uses multiple comparison strategies to avoid false negatives.
        
        Args:
            warranty1: First warranty text
            warranty2: Second warranty text
            threshold: Similarity threshold (0.0 to 1.0)
            
        Returns:
            True if warranties are considered similar
        """
        if not warranty1 or not warranty2:
            return False
        
        # Normalize both texts
        norm1 = ComparisonService._normalize_warranty_text(warranty1)
        norm2 = ComparisonService._normalize_warranty_text(warranty2)
        
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
            logger.debug(f"✓ Warranties similar by character match ({similarity:.2f}): '{warranty1[:50]}' ≈ '{warranty2[:50]}'")
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
            'pump': ['pump', 'fire pump', 'diesel pump', 'water pump']
        }
        
        # Find which key phrase categories match
        w1_categories = set()
        w2_categories = set()
        
        for category, phrases in key_phrases.items():
            if any(phrase in norm1 for phrase in phrases):
                w1_categories.add(category)
            if any(phrase in norm2 for phrase in phrases):
                w2_categories.add(category)
        
        # If they share key phrase categories, they're likely the same warranty
        if w1_categories and w2_categories:
            overlap = w1_categories & w2_categories
            if overlap:
                logger.debug(f"✓ Warranties similar by key phrases ({overlap}): '{warranty1[:50]}' ≈ '{warranty2[:50]}'")
                return True
        
        return False
    
    @staticmethod
    def _find_unique_warranties(provider_warranties: List[str], all_quotes: List[ExtractedQuoteData], provider_name: str) -> List[str]:
        """
        Find warranties that are truly unique to this provider.
        Uses semantic comparison to avoid false positives from text variations.
        
        Args:
            provider_warranties: List of warranties for the provider
            all_quotes: List of all quotes being compared
            provider_name: Name of the provider to find unique warranties for
            
        Returns:
            List of unique warranties for this provider
        """
        if not provider_warranties:
            return []
        
        # Collect all warranties from OTHER providers
        other_warranties = []
        for quote in all_quotes:
            if quote.company_name != provider_name and quote.warranties:
                other_warranties.extend(quote.warranties)
        
        if not other_warranties:
            # If no other warranties exist, all are unique (but unlikely)
            return provider_warranties
        
        unique_warranties = []
        
        for warranty in provider_warranties:
            is_unique = True
            
            for other_warranty in other_warranties:
                if ComparisonService._are_warranties_similar(warranty, other_warranty):
                    is_unique = False
                    break
            
            if is_unique:
                unique_warranties.append(warranty)
                logger.info(f"✓ Found unique warranty for {provider_name}: {warranty[:80]}")
        
        return unique_warranties
    
    # ============================================================================
    # NEW: UNIQUE SUBJECTIVITIES DETECTION v7.1
    # ============================================================================
    
    @staticmethod
    def _normalize_subjectivity_text(text: str) -> str:
        """
        Normalize subjectivity text for comparison.
        """
        if not text:
            return ""
        
        text = text.lower()
        
        # Remove common variations
        text = re.sub(r'required?|mandatory|must\s+have|necessary', 'needed', text)
        text = re.sub(r'certificate|certification|license|licence', 'cert', text)
        text = re.sub(r'civil\s+defense|civil\s+defence|cd\s+cert', 'civildefense', text)
        text = re.sub(r'photograph|photo|picture|image', 'photo', text)
        text = re.sub(r'\d+\s*days?', '[TIME]', text)
        
        # Remove punctuation and extra spaces
        text = re.sub(r'[,\.\;\:\(\)\[\]\{\}]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    @staticmethod
    def _are_subjectivities_similar(subj1: str, subj2: str, threshold: float = 0.75) -> bool:
        """
        Check if two subjectivities are semantically similar.
        """
        if not subj1 or not subj2:
            return False
        
        norm1 = ComparisonService._normalize_subjectivity_text(subj1)
        norm2 = ComparisonService._normalize_subjectivity_text(subj2)
        
        if not norm1 or not norm2:
            return False
        
        # Exact match after normalization
        if norm1 == norm2:
            return True
        
        # One contains the other
        if norm1 in norm2 or norm2 in norm1:
            return True
        
        # Character similarity
        similarity = SequenceMatcher(None, norm1, norm2).ratio()
        if similarity >= threshold:
            return True
        
        # Key phrase matching for common subjectivities
        common_subjectivity_patterns = {
            'survey': ['survey', 'inspection', 'risk assessment'],
            'civil_defense': ['civildefense', 'cd cert', 'fire safety'],
            'gps': ['gps', 'coordinates', 'location', 'longitude', 'latitude'],
            'photos': ['photo', 'picture', 'image', 'visual'],
            'valuation': ['valuation', 'appraisal', 'assessment report'],
            'kyc': ['kyc', 'aml', 'know your customer'],
            'sama': ['sama', 'ia', 'insurance authority', 'circular'],
            'business_plan': ['business contingency', 'continuity plan', 'disaster recovery'],
            'building_cert': ['building cert', 'municipality', 'construction permit']
        }
        
        norm1_keys = set()
        norm2_keys = set()
        
        for key, patterns in common_subjectivity_patterns.items():
            if any(p in norm1 for p in patterns):
                norm1_keys.add(key)
            if any(p in norm2 for p in patterns):
                norm2_keys.add(key)
        
        if norm1_keys and norm2_keys and (norm1_keys & norm2_keys):
            return True
        
        return False
    
    @staticmethod
    def _flatten_subjectivities(quote: ExtractedQuoteData) -> List[str]:
        """
        Flatten all subjectivities from a quote into a single list.
        Extracts from extended data structure.
        """
        all_subjectivities = []
        
        # Try to get from subscriptions field (might be typo for subjectivities)
        if hasattr(quote, 'subscriptions') and quote.subscriptions:
            if isinstance(quote.subscriptions, list):
                all_subjectivities.extend(quote.subscriptions)
        
        # Try to get from additional_info
        if hasattr(quote, 'additional_info') and quote.additional_info:
            if isinstance(quote.additional_info, dict):
                # Look for subjectivities in various possible keys
                for key in ['subjectivities', 'subjectivities_and_requirements', 'binding_requirements', 
                            'conditions_precedent', 'documentation_required']:
                    if key in quote.additional_info:
                        data = quote.additional_info[key]
                        if isinstance(data, list):
                            all_subjectivities.extend(data)
                        elif isinstance(data, dict):
                            for sub_key, sub_value in data.items():
                                if isinstance(sub_value, list):
                                    all_subjectivities.extend(sub_value)
        
        return all_subjectivities
    
    @staticmethod
    def _find_unique_subjectivities(provider_subjectivities: List[str], all_quotes: List[ExtractedQuoteData], provider_name: str) -> List[str]:
        """
        Find subjectivities that are unique to this provider.
        
        Args:
            provider_subjectivities: List of subjectivities for the provider
            all_quotes: List of all quotes being compared
            provider_name: Name of the provider
            
        Returns:
            List of unique subjectivities
        """
        if not provider_subjectivities:
            return []
        
        # Collect all subjectivities from OTHER providers
        other_subjectivities = []
        for quote in all_quotes:
            if quote.company_name != provider_name:
                other_subj = ComparisonService._flatten_subjectivities(quote)
                other_subjectivities.extend(other_subj)
        
        if not other_subjectivities:
            return provider_subjectivities
        
        unique_subjectivities = []
        
        for subj in provider_subjectivities:
            is_unique = True
            
            for other_subj in other_subjectivities:
                if ComparisonService._are_subjectivities_similar(subj, other_subj):
                    is_unique = False
                    break
            
            if is_unique:
                unique_subjectivities.append(subj)
                logger.info(f"✓ Found unique subjectivity for {provider_name}: {subj[:80]}")
        
        return unique_subjectivities
    
    # ============================================================================
    # UPDATED: EXTRACT KEY DIFFERENCES v7.1 - WITH UNIQUE ITEMS
    # ============================================================================
    
    @staticmethod
    def extract_key_differences(quotes: List[ExtractedQuoteData]) -> List[Dict]:
        """
        Extract key differences between all quotes.
        NOW INCLUDES: Unique warranties and unique subjectivities per provider.
        
        Args:
            quotes: List of quotes to compare
            
        Returns:
            List of key difference objects with unique items
        """
        if len(quotes) < 2:
            return []
        
        differences = []
        
        # Price differences (existing logic)
        premiums = [(q.company_name, q.premium_amount or 0) for q in quotes if q.premium_amount]
        if premiums:
            premiums_sorted = sorted(premiums, key=lambda x: x[1])
            cheapest = premiums_sorted[0]
            most_expensive = premiums_sorted[-1]
            
            if cheapest[1] < most_expensive[1]:
                diff_amount = most_expensive[1] - cheapest[1]
                diff_pct = (diff_amount / most_expensive[1] * 100) if most_expensive[1] > 0 else 0
                
                differences.append({
                    'category': 'Premium',
                    'description': f"{cheapest[0]} is {diff_pct:.1f}% cheaper than {most_expensive[0]}",
                    'impact': 'high',
                    'savings': diff_amount
                })
        
        # Benefits differences (existing logic)
        benefits_counts = [(q.company_name, len(q.key_benefits)) for q in quotes]
        if benefits_counts:
            benefits_sorted = sorted(benefits_counts, key=lambda x: x[1], reverse=True)
            most_benefits = benefits_sorted[0]
            least_benefits = benefits_sorted[-1]
            
            if most_benefits[1] > least_benefits[1]:
                diff = most_benefits[1] - least_benefits[1]
                differences.append({
                    'category': 'Coverage',
                    'description': f"{most_benefits[0]} offers {diff} more benefits than {least_benefits[0]}",
                    'impact': 'medium'
                })
        
        # Exclusions differences (existing logic)
        exclusions_counts = [(q.company_name, len(q.exclusions)) for q in quotes]
        if exclusions_counts:
            exclusions_sorted = sorted(exclusions_counts, key=lambda x: x[1])
            fewest_exclusions = exclusions_sorted[0]
            most_exclusions = exclusions_sorted[-1]
            
            if most_exclusions[1] > fewest_exclusions[1]:
                diff = most_exclusions[1] - fewest_exclusions[1]
                differences.append({
                    'category': 'Exclusions',
                    'description': f"{fewest_exclusions[0]} has {diff} fewer exclusions than {most_exclusions[0]}",
                    'impact': 'medium'
                })
        
        # NEW: Unique warranties per provider
        logger.info(f"\n🔍 Analyzing unique warranties across {len(quotes)} providers...")
        unique_warranties_by_provider = {}
        
        for quote in quotes:
            if quote.warranties:
                unique_warranties = ComparisonService._find_unique_warranties(
                    quote.warranties,
                    quotes,
                    quote.company_name
                )
                if unique_warranties:
                    unique_warranties_by_provider[quote.company_name] = unique_warranties
                    logger.info(f"   ✓ {quote.company_name}: {len(unique_warranties)} unique warranties")
        
        # Add unique warranties to differences
        if unique_warranties_by_provider:
            differences.append({
                'category': 'Unique Warranties',
                'unique_items_by_provider': unique_warranties_by_provider,
                'impact': 'medium'
            })
        
        # NEW: Unique subjectivities per provider
        logger.info(f"\n🔍 Analyzing unique subjectivities across {len(quotes)} providers...")
        unique_subjectivities_by_provider = {}
        
        for quote in quotes:
            provider_subj = ComparisonService._flatten_subjectivities(quote)
            if provider_subj:
                unique_subj = ComparisonService._find_unique_subjectivities(
                    provider_subj,
                    quotes,
                    quote.company_name
                )
                if unique_subj:
                    unique_subjectivities_by_provider[quote.company_name] = unique_subj
                    logger.info(f"   ✓ {quote.company_name}: {len(unique_subj)} unique subjectivities")
        
        # Add unique subjectivities to differences
        if unique_subjectivities_by_provider:
            differences.append({
                'category': 'Unique Subjectivities',
                'unique_items_by_provider': unique_subjectivities_by_provider,
                'impact': 'low'
            })
        
        return differences
    
    @staticmethod
    def generate_side_by_side_data(quotes: List[ExtractedQuoteData]) -> Dict:
        """
        Generate comprehensive side-by-side comparison data.
        Includes FULL LISTS for benefits, exclusions, warranties, subjectivities, conditions.
        
        Args:
            quotes: List of quotes to compare
            
        Returns:
            Side-by-side comparison structure with full lists
        """
        if not quotes:
            return {}
        
        providers = []
        for quote in quotes:
            client_name = getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
            ia_compliant = getattr(quote, 'ia_compliant', False)
            company_name_ar = getattr(quote, 'company_name_ar', None)
            
            providers.append({
                'name': quote.company_name,
                'name_ar': company_name_ar,
                'client_name': client_name,
                'ia_compliant': ia_compliant,
                'score': quote.score or 0,
                'premium': quote.premium_amount or 0,
                'rate': quote.rate or 'N/A'
            })
        
        comparison_matrix = {
            "premium": [
                {
                    "provider": quote.company_name,
                    "value": quote.premium_amount or 0,
                    "formatted": f"SAR {quote.premium_amount:,.2f}" if quote.premium_amount else "N/A"
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
                    "items": ComparisonService._extract_conditions(quote)
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
            ],
            "client_names": [
                {
                    "provider": quote.company_name,
                    "client": getattr(quote, 'client_name', None) or getattr(quote, 'insured_name', 'Not specified')
                }
                for quote in quotes
            ],
            "ia_compliance": [
                {
                    "provider": quote.company_name,
                    "ia_compliant": getattr(quote, 'ia_compliant', False)
                }
                for quote in quotes
            ]
        }
        
        best_quote = max(quotes, key=lambda q: q.score or 0)
        
        return {
            "providers": providers,
            "comparison_matrix": comparison_matrix,
            "summary": f"Detailed side-by-side comparison of {len(quotes)} providers",
            "winner": best_quote.company_name,
            "winner_reasons": best_quote.strengths[:3] if best_quote.strengths else []
        }
    
    @staticmethod
    def _extract_conditions(quote: ExtractedQuoteData) -> List[str]:
        """Extract conditions from quote data."""
        conditions = []
        
        if quote.additional_info:
            if isinstance(quote.additional_info, dict):
                conditions_data = quote.additional_info.get('conditions', [])
                if isinstance(conditions_data, list):
                    conditions.extend(conditions_data)
            elif isinstance(quote.additional_info, str):
                if 'condition' in quote.additional_info.lower():
                    conditions.append(quote.additional_info)
        
        if not conditions:
            conditions = [
                "Standard policy terms and conditions apply",
                "Subject to final underwriting approval",
                "Premium payment required at inception"
            ]
        
        return conditions
    
    @staticmethod
    def generate_comparison_matrix(quotes: List[ExtractedQuoteData]) -> Dict[str, List[List[Optional[SideBySideComparison]]]]:
        """
        Generate a matrix of comparisons grouped by policy type.
        Only compares quotes of the same type.
        
        Args:
            quotes: List of all quotes
            
        Returns:
            Dictionary with policy type as key, comparison matrix as value
        """
        
        grouped = ComparisonService.group_quotes_by_policy_type(quotes)
        
        all_matrices = {}
        
        for policy_type, quotes_list in grouped.items():
            n = len(quotes_list)
            matrix = []
            
            for i in range(n):
                row = []
                for j in range(n):
                    if i != j:
                        comparison = ComparisonService.compare_two_quotes(quotes_list[i], quotes_list[j])
                        row.append(comparison)
                    else:
                        row.append(None)
                matrix.append(row)
            
            all_matrices[policy_type] = matrix
        
        return all_matrices


def rank_and_compare_quotes(quotes: List[ExtractedQuoteData]) -> Dict:
    """
    Main function to rank and compare quotes.
    Groups by type, ranks within type, provides comparisons.
    NOW INCLUDES: Unique warranties and unique subjectivities detection.
    
    Args:
        quotes: List of extracted quotes
        
    Returns:
        Dictionary with ranking and comparison data including unique items
    """
    service = ComparisonService()
    
    ranked_by_type = service.rank_quotes_by_type(quotes)
    
    key_differences = service.extract_key_differences(quotes)
    
    side_by_side = service.generate_side_by_side_data(quotes)
    
    policy_types_found = list(ranked_by_type.keys())
    mixed_lines_warning = None
    
    if len(policy_types_found) > 1:
        mixed_lines_warning = {
            'warning': True,
            'message': f"⚠️ MIXED INSURANCE LINES DETECTED: {', '.join(policy_types_found)}",
            'recommendation': "Comparing quotes from different insurance types may not be meaningful. For best results, upload quotes of the same insurance type.",
            'policy_types': policy_types_found
        }
        logger.warning(f"⚠️ Mixed insurance lines: {', '.join(policy_types_found)}")
    
    summary = {
        "total_quotes_analyzed": len(quotes),
        "policy_types_found": policy_types_found,
        "mixed_lines_warning": mixed_lines_warning,
        "rankings_by_type": ranked_by_type,
        "key_differences": {
            "differences": key_differences,
            "summary": f"Comparison of {len(quotes)} providers with detailed differences",
            "recommendation": side_by_side["winner"] + " offers the best overall value"
        },
        "side_by_side": side_by_side,
        "comparison_note": "Quotes are grouped by policy type. Only quotes of the same type are compared against each other."
    }
    
    if len(ranked_by_type) == 1:
        policy_type = list(ranked_by_type.keys())[0]
        ranked_list = ranked_by_type[policy_type]
        if ranked_list:
            summary["best_overall"] = ranked_list[0]["company"]
            summary["best_overall_client"] = ranked_list[0]["client_name"]
            summary["best_value"] = min(ranked_list, key=lambda x: x["premium"])["company"]
    
    return summary