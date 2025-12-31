"""
PDF Extraction Service
======================
Handles text and logo extraction from PDF documents.
"""

import logging
import base64
from io import BytesIO
from typing import Optional, Tuple
from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image

from app.core.config import settings

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Service for extracting text and images from PDF files"""
    
    @staticmethod
    def extract_text_from_pdf(file_path: str) -> str:
        """
        Extract text content from a PDF file.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Extracted text content
            
        Raises:
            Exception: If PDF extraction fails
        """
        try:
            logger.info(f"Extracting text from PDF: {file_path}")
            
            # Open the PDF document
            doc = fitz.open(file_path)
            text_content = []
            
            # Extract text from each page
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text()
                text_content.append(text)
            
            doc.close()
            
            # Combine all pages
            full_text = "\n\n".join(text_content)
            
            logger.info(f"Successfully extracted {len(full_text)} characters from {len(text_content)} pages")
            
            if not full_text.strip():
                logger.warning(f"No text found in PDF: {file_path}")
                return "No extractable text found in PDF"
            
            return full_text
            
        except Exception as e:
            logger.error(f"Error extracting text from PDF {file_path}: {str(e)}")
            raise Exception(f"Failed to extract text from PDF: {str(e)}")
    
    @staticmethod
    def extract_logo_from_pdf(file_path: str) -> Optional[str]:
        """
        Extract the company logo (first substantial image) from PDF.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Base64 encoded logo image or None
        """
        if not settings.ENABLE_LOGO_EXTRACTION:
            return None
            
        try:
            logger.info(f"Attempting to extract logo from PDF: {file_path}")
            
            doc = fitz.open(file_path)
            
            # Check first 2 pages for logos (usually on first page)
            for page_num in range(min(2, len(doc))):
                page = doc[page_num]
                image_list = page.get_images(full=True)
                
                # Look for suitable logo images
                for img_index, img in enumerate(image_list):
                    try:
                        xref = img[0]
                        base_image = doc.extract_image(xref)
                        image_bytes = base_image["image"]
                        
                        # Skip very small or very large images
                        if len(image_bytes) < 1000 or len(image_bytes) > settings.MAX_LOGO_SIZE:
                            continue
                        
                        # Try to open and validate the image
                        image = Image.open(BytesIO(image_bytes))
                        width, height = image.size
                        
                        # Logo typically in header, reasonable dimensions
                        # Aspect ratio and size checks for typical logos
                        if 50 < width < 800 and 50 < height < 400:
                            # Convert to PNG format for consistency
                            buffer = BytesIO()
                            image.save(buffer, format="PNG")
                            img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                            
                            doc.close()
                            logger.info(f"Successfully extracted logo from PDF page {page_num + 1}")
                            return f"data:image/png;base64,{img_base64}"
                    
                    except Exception as img_error:
                        logger.debug(f"Skipping image {img_index}: {str(img_error)}")
                        continue
            
            doc.close()
            logger.info("No suitable logo found in PDF")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting logo from PDF {file_path}: {str(e)}")
            return None
    
    @staticmethod
    def validate_pdf(file_path: str) -> Tuple[bool, Optional[str]]:
        """
        Validate if a file is a proper PDF.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check file extension
            if not file_path.lower().endswith('.pdf'):
                return False, "File must be a PDF"
            
            # Check file exists and size
            path = Path(file_path)
            if not path.exists():
                return False, "File does not exist"
            
            file_size = path.stat().st_size
            if file_size == 0:
                return False, "File is empty"
            
            if file_size > settings.MAX_FILE_SIZE:
                return False, f"File size exceeds maximum allowed ({settings.MAX_FILE_SIZE / (1024*1024)}MB)"
            
            # Try to open with PyMuPDF
            doc = fitz.open(file_path)
            page_count = len(doc)
            doc.close()
            
            if page_count == 0:
                return False, "PDF has no pages"
            
            return True, None
            
        except Exception as e:
            return False, f"Invalid PDF file: {str(e)}"


# Create singleton instance
pdf_extractor = PDFExtractor()