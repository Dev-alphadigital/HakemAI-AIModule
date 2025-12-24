"""
Initialize Hakim Scores from ai_ranker.py
Extracts all unique companies and their scores from HAKIM_SCORE dictionary
and saves them to MongoDB database.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.hakim_score_service import hakim_score_service


# Import HAKIM_SCORE from ai_ranker
from app.services.ai_ranker import HAKIM_SCORE


def extract_unique_companies():
    """
    Extract unique companies from HAKIM_SCORE dictionary.
    Groups aliases together and uses the main company name.
    Returns list of unique companies with their scores.
    """
    companies_by_rank = {}
    
    # Group companies by rank (same rank = same company with different aliases)
    for company_name, data in HAKIM_SCORE.items():
        rank = data.get('rank', 999)
        score = data.get('score', 0.75)
        tier = data.get('tier', 'Standard')
        
        if rank not in companies_by_rank:
            companies_by_rank[rank] = {
                'names': [],
                'score': score,
                'tier': tier,
                'rank': rank
            }
        
        companies_by_rank[rank]['names'].append(company_name)
    
    # Extract unique companies - use the longest name as the main name
    unique_companies = []
    for rank in sorted(companies_by_rank.keys()):
        company_data = companies_by_rank[rank]
        names = company_data['names']
        
        # Find the longest name (usually the full company name)
        main_name = max(names, key=len)
        
        # Get aliases (all other names for this company)
        aliases = [name for name in names if name != main_name]
        
        # Filter out Arabic names and very short abbreviations from main name candidates
        # Prefer English names over Arabic, and longer names
        english_names = [n for n in names if not any('\u0600' <= c <= '\u06FF' for c in n)]
        if english_names:
            # Among English names, prefer the longest one that's not too short
            candidates = [n for n in english_names if len(n) > 3]
            if candidates:
                main_name = max(candidates, key=len)
                aliases = [name for name in names if name != main_name]
        
        unique_companies.append({
            'company_name': main_name,
            'score': company_data['score'],
            'tier': company_data['tier'],
            'rank': company_data['rank'],
            'aliases': aliases
        })
    
    return unique_companies


async def initialize_database():
    """Initialize database with all companies from HAKIM_SCORE."""
    try:
        # Connect to MongoDB
        print("🔌 Connecting to MongoDB...")
        await hakim_score_service.connect()
        print("✅ Connected to MongoDB")
        
        # Extract unique companies
        print("\n📊 Extracting unique companies from HAKIM_SCORE...")
        companies = extract_unique_companies()
        print(f"✅ Found {len(companies)} unique companies")
        
        # Prepare data for bulk insert
        scores_data = [
            {
                'company_name': company['company_name'],
                'score': company['score'],
                'tier': company['tier'],
                'rank': company['rank'],
                'aliases': company['aliases']
            }
            for company in companies
        ]
        
        # Bulk create/update
        print(f"\n💾 Saving {len(scores_data)} companies to database...")
        result = await hakim_score_service.bulk_create_or_update(scores_data)
        
        print(f"\n✅ Bulk operation completed!")
        print(f"   Created: {result['created']}")
        print(f"   Updated: {result['updated']}")
        print(f"   Failed: {result['failed']}")
        print(f"   Total: {result['total']}")
        
        if result['failed'] > 0:
            print("\n⚠️  Failed companies:")
            for item in result.get('results', []):
                if not item.get('success', False):
                    print(f"   - {item.get('company_name', 'Unknown')}: {item.get('error', 'Unknown error')}")
        
        # Verify - get all companies
        print("\n📋 Verifying saved companies...")
        all_scores = await hakim_score_service.get_all_hakim_scores(
            sort_by="rank",
            sort_order=1,
            include_zero_scores=True
        )
        print(f"✅ Database contains {len(all_scores)} companies")
        
        # Print first 10 companies as verification
        print("\n📝 First 10 companies in database:")
        for i, company in enumerate(all_scores[:10], 1):
            print(f"   {i}. {company['company_name']}: {company['score']:.2f} ({company['tier']})")
        
        if len(all_scores) > 10:
            print(f"   ... and {len(all_scores) - 10} more companies")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Disconnect
        await hakim_score_service.disconnect()
        print("\n🔌 Disconnected from MongoDB")


if __name__ == "__main__":
    print("=" * 70)
    print("HAKIM SCORE DATABASE INITIALIZATION")
    print("=" * 70)
    print()
    print("This script will:")
    print("  1. Extract all unique companies from HAKIM_SCORE in ai_ranker.py")
    print("  2. Save them to MongoDB database")
    print("  3. Group aliases together (e.g., 'GIG' and 'Gulf Insurance Group')")
    print()
    
    asyncio.run(initialize_database())
    
    print("\n" + "=" * 70)
    print("✅ INITIALIZATION COMPLETE")
    print("=" * 70)
