"""
Test script to demonstrate company name matching capabilities.
Tests various cases: case variations, Arabic names, abbreviations, etc.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.hakim_score_service import hakim_score_service


async def test_matching():
    """Test various company name matching scenarios."""
    
    await hakim_score_service.connect()
    
    # Test cases: (input_name, expected_company)
    test_cases = [
        # Case variations
        ("GIG", "Gulf Insurance Group"),
        ("gig", "Gulf Insurance Group"),
        ("Gig", "Gulf Insurance Group"),
        ("GULF INSURANCE GROUP", "Gulf Insurance Group"),
        ("gulf insurance group", "Gulf Insurance Group"),
        
        # Arabic names
        ("التعاونية", "The Company for Cooperative Insurance (Tawuniya)"),
        ("ولاء", "Walaa Cooperative Insurance Company"),
        ("الوطنية", "Wataniya Insurance"),
        ("ميدغلف", "Mediterranean and Gulf Insurance"),
        
        # Full names
        ("Gulf Insurance Group", "Gulf Insurance Group"),
        ("The Company for Cooperative Insurance (Tawuniya)", "The Company for Cooperative Insurance (Tawuniya)"),
        
        # Variations with punctuation
        ("Al-Etihad Cooperative Insurance Co.", "Al-Etihad Cooperative Insurance Co."),
        ("Al Etihad", "Al-Etihad Cooperative Insurance Co."),
        ("AL-Etihad", "Al-Etihad Cooperative Insurance Co."),
        
        # Abbreviations
        ("UCA", "United Cooperative Assurance Company"),
        ("AXA", "AXA Cooperative Insurance Company"),
        ("MedGulf", "Mediterranean and Gulf Insurance"),
    ]
    
    print("=" * 70)
    print("COMPANY NAME MATCHING TEST")
    print("=" * 70)
    print()
    
    passed = 0
    failed = 0
    
    for input_name, expected_company in test_cases:
        try:
            result = await hakim_score_service.get_hakim_score(input_name)
            
            if result:
                actual_company = result.get('company_name', 'N/A')
                score = result.get('score', 0.0)
                tier = result.get('tier', 'N/A')
                
                if actual_company == expected_company:
                    print(f"✅ PASS: '{input_name}' → '{actual_company}' (Score: {score:.2f}, Tier: {tier})")
                    passed += 1
                else:
                    print(f"❌ FAIL: '{input_name}'")
                    print(f"   Expected: '{expected_company}'")
                    print(f"   Got:      '{actual_company}' (Score: {score:.2f}, Tier: {tier})")
                    failed += 1
            else:
                print(f"❌ FAIL: '{input_name}' → No match found")
                print(f"   Expected: '{expected_company}'")
                failed += 1
        except Exception as e:
            print(f"❌ ERROR testing '{input_name}': {e}")
            failed += 1
        
        print()
    
    print("=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)
    
    await hakim_score_service.disconnect()


if __name__ == "__main__":
    asyncio.run(test_matching())
