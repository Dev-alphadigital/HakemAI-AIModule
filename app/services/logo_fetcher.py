"""
Logo Fetching Service
=====================
Fetches company logos from websites when not found in PDF.
"""

import logging
import requests
import base64
from typing import Optional
from io import BytesIO
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from PIL import Image

logger = logging.getLogger(__name__)


class LogoFetcher:
    """Service for fetching company logos from the web"""
    
    # Common insurance company domains
    KNOWN_DOMAINS = {
        "chubb": "https://www.chubb.com",
        "allianz": "https://www.allianz.com",
        "axa": "https://www.axa.com",
        "zurich": "https://www.zurich.com",
        "metlife": "https://www.metlife.com",
        "prudential": "https://www.prudential.com",
        "aig": "https://www.aig.com",
        "aviva": "https://www.aviva.com",
        "generali": "https://www.generali.com",
        "tokio marine": "https://www.tokiomarine.com",
    }
    
    @staticmethod
    def _get_company_website(company_name: str) -> Optional[str]:
        """
        Attempt to determine company website from name.
        
        Args:
            company_name: Insurance company name
            
        Returns:
            Company website URL or None
        """
        company_lower = company_name.lower()
        
        # Check known domains
        for key, domain in LogoFetcher.KNOWN_DOMAINS.items():
            if key in company_lower:
                return domain
        
        # Try to construct domain from company name
        # Remove common words
        clean_name = company_lower.replace("insurance", "").replace("company", "")
        clean_name = clean_name.replace("cooperative", "").replace("group", "")
        clean_name = clean_name.strip()
        
        # Get first significant word
        words = clean_name.split()
        if words:
            main_word = words[0]
            return f"https://www.{main_word}.com"
        
        return None
    
    @staticmethod
    def _download_image(url: str) -> Optional[bytes]:
        """
        Download image from URL.
        
        Args:
            url: Image URL
            
        Returns:
            Image bytes or None
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200 and 'image' in response.headers.get('content-type', ''):
                return response.content
            
            return None
            
        except Exception as e:
            logger.debug(f"Failed to download image from {url}: {str(e)}")
            return None
    
    @staticmethod
    def _extract_logo_from_website(website_url: str) -> Optional[str]:
        """
        Extract logo from a company website.
        
        Args:
            website_url: Company website URL
            
        Returns:
            Base64 encoded logo or None
        """
        try:
            logger.info(f"Attempting to fetch logo from: {website_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Get website HTML
            response = requests.get(website_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Strategy 1: Look for logo in common locations
            logo_candidates = []
            
            # Check meta tags for logo
            og_image = soup.find('meta', property='og:image')
            if og_image and og_image.get('content'):
                logo_candidates.append(og_image['content'])
            
            # Check for common logo class names/IDs
            logo_selectors = [
                'img[class*="logo"]',
                'img[id*="logo"]',
                'a[class*="logo"] img',
                'div[class*="logo"] img',
                'header img',
                'nav img'
            ]
            
            for selector in logo_selectors:
                imgs = soup.select(selector)
                for img in imgs[:3]:  # Check first 3 matches
                    src = img.get('src') or img.get('data-src')
                    if src:
                        logo_candidates.append(src)
            
            # Try to download and validate each candidate
            for logo_url in logo_candidates:
                # Make URL absolute
                if logo_url.startswith('//'):
                    logo_url = 'https:' + logo_url
                elif logo_url.startswith('/'):
                    parsed = urlparse(website_url)
                    logo_url = f"{parsed.scheme}://{parsed.netloc}{logo_url}"
                elif not logo_url.startswith('http'):
                    continue
                
                # Download image
                img_data = LogoFetcher._download_image(logo_url)
                
                if img_data:
                    # Validate and process image
                    try:
                        image = Image.open(BytesIO(img_data))
                        width, height = image.size
                        
                        # Logo validation: reasonable dimensions
                        if 50 < width < 800 and 50 < height < 400:
                            # Resize if too large
                            if width > 400 or height > 200:
                                image.thumbnail((400, 200), Image.Resampling.LANCZOS)
                            
                            # Convert to PNG
                            buffer = BytesIO()
                            if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
                                image.save(buffer, format="PNG")
                            else:
                                # Convert to RGB if needed
                                rgb_image = image.convert('RGB')
                                rgb_image.save(buffer, format="PNG")
                            
                            img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                            
                            logger.info(f"✅ Successfully fetched logo from website")
                            return f"data:image/png;base64,{img_base64}"
                    
                    except Exception as img_error:
                        logger.debug(f"Invalid image: {str(img_error)}")
                        continue
            
            logger.info("No suitable logo found on website")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching logo from website: {str(e)}")
            return None
    
    @staticmethod
    def fetch_company_logo(company_name: str, existing_logo: Optional[str] = None) -> Optional[str]:
        """
        Main method to fetch company logo.
        First checks if logo already exists (from PDF), then tries website.
        
        Args:
            company_name: Insurance company name
            existing_logo: Logo already extracted from PDF (if any)
            
        Returns:
            Base64 encoded logo or None
        """
        # If we already have a logo from PDF, return it
        if existing_logo:
            logger.info(f"Using logo from PDF for {company_name}")
            return existing_logo
        
        # Try to fetch from website
        website_url = LogoFetcher._get_company_website(company_name)
        
        if not website_url:
            logger.info(f"Could not determine website for {company_name}")
            return None
        
        return LogoFetcher._extract_logo_from_website(website_url)
    
    def get_logo(self, company_name: str, company_website: Optional[str] = None, pdf_logo: Optional[str] = None) -> Optional[str]:
        """
        Get the best available logo for a company.
        This method provides the interface expected by the routes.
        
        Args:
            company_name: Insurance company name
            company_website: Company website URL (if known)
            pdf_logo: Logo extracted from PDF (if any)
            
        Returns:
            Base64 encoded logo or None
        """
        # If we already have a logo from PDF, return it
        if pdf_logo:
            logger.info(f"Using logo from PDF for {company_name}")
            return pdf_logo
        
        # If we have a company website, try to extract logo from it
        if company_website:
            try:
                logo = LogoFetcher._extract_logo_from_website(company_website)
                if logo:
                    logger.info(f"Successfully fetched logo from website for {company_name}")
                    return logo
            except Exception as e:
                logger.warning(f"Failed to fetch logo from provided website {company_website}: {str(e)}")
        
        # Fallback to fetching from company name
        return LogoFetcher.fetch_company_logo(company_name, pdf_logo)


# Create singleton instance
logo_fetcher = LogoFetcher()