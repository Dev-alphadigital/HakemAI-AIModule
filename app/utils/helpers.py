"""
Helper Utilities
================
Common utility functions used across the application.
"""

import re
from typing import Optional, List
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename to remove potentially dangerous characters.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename safe for filesystem use
    """
    # Remove path components
    filename = Path(filename).name
    
    # Remove or replace dangerous characters
    filename = re.sub(r'[^\w\s\-\.]', '', filename)
    filename = re.sub(r'\s+', '_', filename)
    
    # Limit length
    if len(filename) > 200:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = f"{name[:190]}.{ext}" if ext else name[:200]
    
    return filename


def format_currency(amount: Optional[float], currency: str = "SAR") -> str:
    """
    Format a currency amount with proper formatting.
    
    Args:
        amount: Numeric amount
        currency: Currency code (SAR, USD, etc.)
        
    Returns:
        Formatted currency string
    """
    if amount is None:
        return "N/A"
    
    try:
        return f"{currency} {amount:,.2f}"
    except (ValueError, TypeError):
        return f"{currency} {amount}"


def extract_numbers(text: str) -> List[float]:
    """
    Extract all numeric values from a text string.
    
    Args:
        text: Input text
        
    Returns:
        List of extracted numbers
    """
    if not text:
        return []
    
    # Pattern to match numbers with optional decimals and commas
    pattern = r'\d+(?:,\d{3})*(?:\.\d+)?'
    matches = re.findall(pattern, str(text))
    
    numbers = []
    for match in matches:
        try:
            # Remove commas and convert to float
            num = float(match.replace(',', ''))
            numbers.append(num)
        except ValueError:
            continue
    
    return numbers


def truncate_text(text: str, max_length: int = 500) -> str:
    """
    Truncate text to a maximum length with ellipsis.
    
    Args:
        text: Input text
        max_length: Maximum length
        
    Returns:
        Truncated text
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length - 3] + "..."


def parse_premium_frequency(frequency_text: Optional[str]) -> str:
    """
    Normalize premium frequency text.
    
    Args:
        frequency_text: Raw frequency text
        
    Returns:
        Normalized frequency (monthly, annual, quarterly, one-time)
    """
    if not frequency_text:
        return "unknown"
    
    text = frequency_text.lower()
    
    if "month" in text:
        return "monthly"
    elif "year" in text or "annual" in text:
        return "annual"
    elif "quarter" in text:
        return "quarterly"
    elif "semi" in text:
        return "semi-annual"
    elif "week" in text:
        return "weekly"
    else:
        return "one-time"