import logging
import os
import re
import zipfile
from typing import Dict, Any, List, Optional
from datetime import datetime
from io import BytesIO
from pathlib import Path

# Optional import for reportlab - only needed when generating PDFs
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch, cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image, KeepTogether
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.colors import HexColor
    from reportlab.platypus.frames import Frame
    from reportlab.platypus.doctemplate import PageTemplate, BaseDocTemplate
    from reportlab.lib.utils import ImageReader
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("⚠️  ReportLab not installed. PDF generation will not be available. Install with: pip install reportlab==4.2.5")

logger = logging.getLogger(__name__)


class BorderedDocTemplate(BaseDocTemplate):
    """Custom document template with decorative borders on all pages."""
    
    def __init__(self, *args, **kwargs):
        BaseDocTemplate.__init__(self, *args, **kwargs)
        self.border_color = HexColor('#2D5016')  # Dark green
        self.border_width = 2
        
        # Create a default page template with frame
        frame = Frame(
            self.leftMargin,
            self.bottomMargin,
            self.width,
            self.height,
            id='normal'
        )
        
        # Add page template
        template = PageTemplate(id='bordered', frames=[frame], onPage=self._on_page)
        self.addPageTemplates([template])
    
    def _on_page(self, canvas, doc):
        """Called for each page - we'll draw borders in afterPage instead."""
        pass
        
    def afterPage(self):
        """Draw borders and page numbers after each page is created."""
        self.canv.saveState()
        
        # Draw full page borders
        border_width = self.border_width
        margin = 0.5 * inch
        
        self.canv.setStrokeColor(self.border_color)
        self.canv.setLineWidth(border_width)
        
        # Draw full rectangle border around the page
        self.canv.rect(
            margin,
            margin,
            self.pagesize[0] - 2 * margin,
            self.pagesize[1] - 2 * margin
        )
        
        # Draw page number at the bottom center (skip first page - cover page)
        page_number = self.canv.getPageNumber()
        if page_number > 1:
            self.canv.setFont("Helvetica", 9)
            self.canv.setFillColor(colors.black)
            self.canv.drawCentredString(
                self.pagesize[0] / 2,
                margin / 2,
                f"Page {page_number - 1}"
            )
        
        self.canv.restoreState()


class PDFGeneratorService:
    """
    Service for generating comprehensive PDF reports from comparison data.
    
    ✨ ENHANCED VERSION with:
    - Strategic 1-page Executive Memo for decision makers
    - Detailed Comparison Analysis with risk assessment, value optimization
    - Enhanced formatting and professional styling
    - Comprehensive error handling and validation
    """
    
    def __init__(self):
        """Initialize PDF generator service."""
        if not REPORTLAB_AVAILABLE:
            logger.warning("⚠️  PDF Generator Service initialized but ReportLab is not available")
            self.page_width, self.page_height = None, None
            self.styles = None
            return
        
        self.page_width, self.page_height = letter
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        logger.info("✅ PDF Generator Service initialized (Enhanced Version)")
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles for professional formatting."""
        if not REPORTLAB_AVAILABLE or self.styles is None:
            return
        
        # Helper function to add style only if it doesn't exist
        def add_style_if_not_exists(name, style_obj):
            if name not in self.styles.byName:
                self.styles.add(style_obj)
        
        # Title style - REDUCED SPACING
        add_style_if_not_exists('CustomTitle', ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=20,  # Reduced from 24
            textColor=HexColor('#2D5016'),  # Dark green
            spaceAfter=15,  # Reduced from 30
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        # Section heading - REDUCED SPACING
        add_style_if_not_exists('SectionHeading', ParagraphStyle(
            name='SectionHeading',
            parent=self.styles['Heading2'],
            fontSize=14,  # Reduced from 16
            textColor=HexColor('#4A7C2A'),  # Medium green
            spaceAfter=8,  # Reduced from 12
            spaceBefore=12,  # Reduced from 20
            fontName='Helvetica-Bold'
        ))
        
        # Subsection heading - REDUCED SPACING
        add_style_if_not_exists('SubsectionHeading', ParagraphStyle(
            name='SubsectionHeading',
            parent=self.styles['Heading3'],
            fontSize=12,  # Reduced from 14
            textColor=HexColor('#6B9F3D'),  # Light green
            spaceAfter=6,  # Reduced from 8
            spaceBefore=8,  # Reduced from 12
            fontName='Helvetica-Bold'
        ))
        
        # Custom body text (use different name to avoid conflict)
        add_style_if_not_exists('CustomBodyText', ParagraphStyle(
            name='CustomBodyText',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.black,
            spaceAfter=6,
            alignment=TA_LEFT
        ))
        
        # Highlighted text
        add_style_if_not_exists('Highlight', ParagraphStyle(
            name='Highlight',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=HexColor('#2D5016'),
            fontName='Helvetica-Bold'
        ))
        
        # Company name style
        add_style_if_not_exists('CompanyName', ParagraphStyle(
            name='CompanyName',
            parent=self.styles['Normal'],
            fontSize=12,
            textColor=HexColor('#2D5016'),
            fontName='Helvetica-Bold',
            spaceAfter=4
        ))
    
    def _get_logo_path(self) -> Optional[str]:
        """Get logo file path if available."""
        # Try common logo locations
        possible_paths = [
            "logo.png",
            "logo.jpg",
            "assets/logo.png",
            "static/logo.png",
            "app/static/logo.png",
            "hakem_logo.png",
            "hakem-ai-logo.png"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    def _create_logo_element(self, width: float = 1.5*inch) -> Optional[Image]:
        """Create logo image element or text placeholder."""
        logo_path = self._get_logo_path()
        
        if logo_path and os.path.exists(logo_path):
            try:
                img = Image(logo_path, width=width, height=width, kind='proportional')
                return img
            except Exception as e:
                logger.warning(f"⚠️  Could not load logo image: {e}")
        
        # Return None if no logo - we'll use text instead
        return None
    
    def _validate_comparison_data(self, comparison_data: Dict[str, Any]) -> None:
        """Validate and sanitize comparison data structure."""
        if not isinstance(comparison_data, dict):
            logger.warning("⚠️  comparison_data is not a dict, converting...")
            comparison_data = {}
        
        # Ensure all expected keys exist with safe defaults
        if "summary" not in comparison_data:
            comparison_data["summary"] = {}
        if "key_differences" not in comparison_data:
            comparison_data["key_differences"] = {}
        if "data_table" not in comparison_data:
            comparison_data["data_table"] = {}
        if "side_by_side" not in comparison_data:
            comparison_data["side_by_side"] = {}
        if "analytics" not in comparison_data:
            comparison_data["analytics"] = {}
        
        # Validate summary structure
        summary = comparison_data.get("summary", {})
        if not isinstance(summary, dict):
            comparison_data["summary"] = {}
        else:
            ranking = summary.get("ranking", [])
            if not isinstance(ranking, list):
                summary["ranking"] = []
            else:
                # Ensure all ranking items are dicts
                summary["ranking"] = [item for item in ranking if isinstance(item, dict)]
        
        # Validate data_table structure
        data_table = comparison_data.get("data_table", {})
        if not isinstance(data_table, dict):
            comparison_data["data_table"] = {}
        else:
            rows = data_table.get("rows", [])
            if not isinstance(rows, list):
                data_table["rows"] = []
            else:
                # Ensure all rows are dicts or lists
                data_table["rows"] = [row for row in rows if isinstance(row, (dict, list))]
            
            columns = data_table.get("columns", [])
            if not isinstance(columns, list):
                data_table["columns"] = []
        
        logger.info(f"✅ Comparison data validated: summary={bool(comparison_data.get('summary'))}, "
                   f"data_table={bool(comparison_data.get('data_table'))}, "
                   f"side_by_side={bool(comparison_data.get('side_by_side'))}")
    
    def generate_comparison_pdf(
        self,
        comparison_data: Dict[str, Any],
        comparison_id: str
    ) -> BytesIO:
        """
        Generate comprehensive PDF report from comparison data.
        
        ✨ ENHANCED with Strategic Memo and Detailed Analysis sections
        
        Args:
            comparison_data: Complete comparison data from API
            comparison_id: Comparison ID for filename
            
        Returns:
            BytesIO buffer containing PDF bytes
            
        Raises:
            ImportError: If reportlab is not installed
        """
        if not REPORTLAB_AVAILABLE:
            raise ImportError(
                "ReportLab is not installed. Please install it with: pip install reportlab==4.2.5"
            )
        
        try:
            # Validate and sanitize comparison data
            logger.info(f"🔍 Validating comparison data for: {comparison_id}")
            self._validate_comparison_data(comparison_data)
            buffer = BytesIO()
            doc = BorderedDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=0.75*inch,
                leftMargin=0.75*inch,
                topMargin=1*inch,
                bottomMargin=0.75*inch
            )
            
            # Build PDF content
            story = []
            
            # Cover page with logo and company names
            try:
                logger.info("📄 Building cover page...")
                story.extend(self._build_cover_page(comparison_data, comparison_id))
                story.append(PageBreak())
                logger.info("✅ Cover page built successfully")
            except Exception as e:
                logger.error(f"❌ Error building cover page: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # Add fallback cover page
                story.append(Paragraph("Insurance Quote Comparison Report", self.styles['CustomTitle']))
                story.append(PageBreak())
            
            # ✨ STRATEGIC MEMO: 1-page Executive Brief (high-level summary)
            try:
                logger.info("📄 Building strategic executive memo (1-page brief)...")
                story.extend(self._build_strategic_memo(comparison_data))
                logger.info("✅ Strategic memo built successfully")
            except Exception as e:
                logger.error(f"❌ Error building strategic memo: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # ✨ DETAILED ANALYSIS: Granular technical report with recommendations
            # (Page break handled inside _build_detailed_comparison_factors)
            try:
                logger.info("📄 Building detailed comparison factors...")
                story.extend(self._build_detailed_comparison_factors(comparison_data))
                story.append(Spacer(1, 0.3*inch))
                logger.info("✅ Detailed comparison factors built successfully")
            except Exception as e:
                logger.error(f"❌ Error building detailed comparison factors: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Key Differences section
            try:
                logger.info("📄 Building key differences section...")
                story.extend(self._build_key_differences_section(comparison_data))
                story.append(Spacer(1, 0.3*inch))
                logger.info("✅ Key differences section built successfully")
            except Exception as e:
                logger.error(f"❌ Error building key differences section: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Data Table section (improved formatting)
            try:
                logger.info("📄 Building data table section...")
                story.extend(self._build_data_table_section(comparison_data))
                story.append(Spacer(1, 0.3*inch))
                logger.info("✅ Data table section built successfully")
            except Exception as e:
                logger.error(f"❌ Error building data table section: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Side-by-Side section
            try:
                logger.info("📄 Building side-by-side section...")
                story.extend(self._build_side_by_side_section(comparison_data))
                story.append(Spacer(1, 0.3*inch))
                logger.info("✅ Side-by-side section built successfully")
            except Exception as e:
                logger.error(f"❌ Error building side-by-side section: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Analytics/Charts section (improved formatting)
            try:
                logger.info("📄 Building analytics section...")
                story.extend(self._build_analytics_section(comparison_data))
                logger.info("✅ Analytics section built successfully")
            except Exception as e:
                logger.error(f"❌ Error building analytics section: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Build PDF
            doc.build(story)
            buffer.seek(0)
            
            logger.info(f"✅ Generated enhanced PDF for comparison: {comparison_id}")
            return buffer
            
        except IndexError as e:
            logger.error(f"❌ Index error generating PDF: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise Exception(f"PDF generation failed: list index out of range - {str(e)}")
        except Exception as e:
            logger.error(f"❌ Error generating PDF: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def generate_strategic_memo_pdf(
        self,
        comparison_data: Dict[str, Any],
        comparison_id: str
    ) -> BytesIO:
        """
        Generate 1-page strategic memo PDF report.
        
        This is a high-level executive brief optimized for decision makers.
        
        Args:
            comparison_data: Complete comparison data from API
            comparison_id: Comparison ID for filename
            
        Returns:
            BytesIO buffer containing PDF bytes
        """
        if not REPORTLAB_AVAILABLE:
            raise ImportError(
                "ReportLab is not installed. Please install it with: pip install reportlab==4.2.5"
            )
        
        try:
            logger.info(f"🔍 Generating 1-page strategic memo for: {comparison_id}")
            self._validate_comparison_data(comparison_data)
            buffer = BytesIO()
            doc = BorderedDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=0.75*inch,
                leftMargin=0.75*inch,
                topMargin=1*inch,
                bottomMargin=0.75*inch
            )
            
            story = []
            
            # Cover page with logo
            try:
                logger.info("📄 Building cover page for strategic memo...")
                story.extend(self._build_cover_page(comparison_data, comparison_id))
                story.append(PageBreak())
                logger.info("✅ Cover page built successfully")
            except Exception as e:
                logger.error(f"❌ Error building cover page: {e}")
                story.append(Paragraph("Insurance Quote Comparison Report", self.styles['CustomTitle']))
                story.append(PageBreak())
            
            # Strategic memo (1-page only)
            try:
                logger.info("📄 Building strategic executive memo (1-page brief)...")
                memo_content = self._build_strategic_memo(comparison_data)
                if memo_content:
                    story.extend(memo_content)
                    logger.info(f"✅ Strategic memo built successfully with {len(memo_content)} elements")
                else:
                    logger.warning("⚠️  Strategic memo returned empty content, adding fallback")
                    story.append(Paragraph("Strategic Analysis", self.styles['SectionHeading']))
                    story.append(Paragraph("Please refer to the detailed comparison report for comprehensive analysis.", self.styles['CustomBodyText']))
            except Exception as e:
                logger.error(f"❌ Error building strategic memo: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # Add fallback content so PDF still generates
                story.append(Paragraph("Strategic Analysis", self.styles['SectionHeading']))
                story.append(Paragraph("An error occurred while generating the strategic memo. Please refer to the detailed comparison report.", self.styles['CustomBodyText']))
            
            # Build PDF
            doc.build(story)
            buffer.seek(0)
            
            logger.info(f"✅ Generated strategic memo PDF for comparison: {comparison_id}")
            return buffer
            
        except Exception as e:
            logger.error(f"❌ Error generating strategic memo PDF: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def generate_detailed_comparison_pdf(
        self,
        comparison_data: Dict[str, Any],
        comparison_id: str
    ) -> BytesIO:
        """
        Generate detailed comparison PDF report.
        
        This is a comprehensive technical report with all comparison details.
        
        Args:
            comparison_data: Complete comparison data from API
            comparison_id: Comparison ID for filename
            
        Returns:
            BytesIO buffer containing PDF bytes
        """
        if not REPORTLAB_AVAILABLE:
            raise ImportError(
                "ReportLab is not installed. Please install it with: pip install reportlab==4.2.5"
            )
        
        try:
            logger.info(f"🔍 Generating detailed comparison PDF for: {comparison_id}")
            self._validate_comparison_data(comparison_data)
            buffer = BytesIO()
            doc = BorderedDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=0.75*inch,
                leftMargin=0.75*inch,
                topMargin=1*inch,
                bottomMargin=0.75*inch
            )
            
            story = []
            
            # Cover page with logo
            try:
                logger.info("📄 Building cover page for detailed comparison...")
                story.extend(self._build_cover_page(comparison_data, comparison_id))
                # Note: PageBreak is handled inside _build_detailed_comparison_factors
                logger.info("✅ Cover page built successfully")
            except Exception as e:
                logger.error(f"❌ Error building cover page: {e}")
                story.append(Paragraph("Insurance Quote Comparison Report", self.styles['CustomTitle']))
                # Note: PageBreak is handled inside _build_detailed_comparison_factors
            
            # Detailed Analysis section (this will add its own PageBreak to start on new page)
            try:
                logger.info("📄 Building detailed comparison factors...")
                story.extend(self._build_detailed_comparison_factors(comparison_data))
                story.append(Spacer(1, 0.3*inch))
                logger.info("✅ Detailed comparison factors built successfully")
            except Exception as e:
                logger.error(f"❌ Error building detailed comparison factors: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Analytics/Charts section (keep only overall score comparison, remove other duplicated tables)
            try:
                logger.info("📄 Building analytics section (overall score comparison only)...")
                story.extend(self._build_analytics_section(comparison_data))
                logger.info("✅ Analytics section built successfully")
            except Exception as e:
                logger.error(f"❌ Error building analytics section: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Build PDF
            doc.build(story)
            buffer.seek(0)
            
            logger.info(f"✅ Generated detailed comparison PDF for comparison: {comparison_id}")
            return buffer
            
        except Exception as e:
            logger.error(f"❌ Error generating detailed comparison PDF: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _build_cover_page(
        self,
        comparison_data: Dict[str, Any],
        comparison_id: str
    ) -> List:
        """Build minimalist cover page with centered logo and title."""
        story = []
        
        # Add some top spacing
        story.append(Spacer(1, 2*inch))
        
        # Logo at top (larger and centered)
        logo = self._create_logo_element(width=3*inch)
        if logo:
            logo.hAlign = 'CENTER'
            story.append(logo)
            story.append(Spacer(1, 0.5*inch))
        else:
            # Text logo if image not available
            logo_text = Paragraph("HAKEM.AI", self.styles['CustomTitle'])
            logo_text.alignment = TA_CENTER
            story.append(logo_text)
            story.append(Spacer(1, 0.5*inch))
        
        # Centered title
        title = Paragraph("AI Powered Comparison Report", self.styles['CustomTitle'])
        title.alignment = TA_CENTER
        story.append(title)
        
        # Add date info (comparison ID removed per user request)
        story.append(Spacer(1, 0.3*inch))
        date_text = Paragraph(
            f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
            ParagraphStyle(
                'CoverDate',
                parent=self.styles['Normal'],
                fontSize=10,
                textColor=colors.grey,
                alignment=TA_CENTER,
                spaceAfter=12
            )
        )
        story.append(date_text)
        
        # Add bottom spacing to ensure content is visible
        story.append(Spacer(1, 1*inch))
        
        return story
    
    def _build_strategic_memo(self, comparison_data: Dict[str, Any]) -> List:
        """
        ✨ STRATEGIC MEMO: 1-page high-level Executive Brief for decision makers.
        Optimized to fit exactly 1 page with concise strategic insights.
        """
        story = []
        
        # Memo header - smaller to save space
        memo_header = Paragraph("EXECUTIVE BRIEF", self.styles['CustomTitle'])
        story.append(memo_header)
        story.append(Spacer(1, 0.15*inch))
        
        # To/From/Date/Subject section - more compact
        memo_info_style = ParagraphStyle(
            'MemoInfo',
            parent=self.styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            spaceAfter=3,
            leftIndent=0.3*inch
        )
        
        # Extract line of insurance from comparison data
        line_of_insurance = "Property Insurance"  # Default
        extracted_quotes = comparison_data.get("extracted_quotes", [])
        if extracted_quotes and len(extracted_quotes) > 0:
            # Try to get from first quote
            first_quote = extracted_quotes[0]
            line_of_insurance = first_quote.get("policy_type") or first_quote.get("insurance_type") or first_quote.get("line_of_business") or "Property Insurance"
        
        memo_info = [
            f"<b>TO:</b> Decision Makers | <b>DATE:</b> {datetime.now().strftime('%B %d, %Y')}",
            f"<b>SUBJECT:</b> Insurance Quote Comparison - Strategic Recommendation",
            f"<b>Line of Insurance:</b> {line_of_insurance}"
        ]
        
        for info in memo_info:
            story.append(Paragraph(info, memo_info_style))
        
        story.append(Spacer(1, 0.15*inch))
        
        # Executive Summary - Compact version
        summary = comparison_data.get("summary", {})
        key_differences = comparison_data.get("key_differences", {})
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])  # Get rows early for use in recommendation
        
        story.append(Paragraph("STRATEGIC ANALYSIS", self.styles['SectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        # Get key metrics
        ranking = summary.get("ranking", [])
        total_providers = len(ranking)
        
        if ranking:
            best_provider = ranking[0] if ranking else {}
            best_name = best_provider.get("company", "N/A")
            best_score = best_provider.get("score", 0)
            best_premium = best_provider.get("premium", 0)
            
            # Calculate price range
            premiums = [r.get("premium", 0) for r in ranking if isinstance(r, dict)]
            price_range_low = min(premiums) if premiums else 0
            price_range_high = max(premiums) if premiums else 0
            price_variance = ((price_range_high - price_range_low) / price_range_low * 100) if price_range_low > 0 else 0
            
            # Show actual number of providers analyzed
            actual_provider_count = len(ranking)
            providers_to_show = min(actual_provider_count, 8)  # Table should fit 8 companies
            
            # Strategic overview - show actual count
            overview_text = f"""
            Analyzed <b>{actual_provider_count} providers</b>. <b>{best_name}</b> ranks #1 with score {best_score:.1f} at SAR {best_premium:,.2f}. 
            Premium variance: {price_variance:.1f}% (SAR {price_range_low:,.2f} - SAR {price_range_high:,.2f}).
            """
            story.append(Paragraph(overview_text, self.styles['CustomBodyText']))
            story.append(Spacer(1, 0.1*inch))
        
        # Strategic Recommendation - more compact
        recommendation = key_differences.get("recommendation", "")
        recommendation_reasoning = key_differences.get("recommendation_reasoning", "")
        
        # Extract deductible information from data
        deductibles = []
        try:
            for row in rows:
                if isinstance(row, dict):
                    deductible = row.get("deductible") or row.get("deductible_amount")
                    if deductible:
                        deductibles.append(deductible)
        except Exception as e:
            logger.warning(f"⚠️  Error extracting deductibles: {e}")
            deductibles = []
        
        # If no recommendation from key_differences, try to get from ranking
        if not recommendation and ranking:
            best_provider = ranking[0] if ranking else {}
            recommendation = best_provider.get("company", "Top-ranked provider")
        
        if recommendation:
            # Update reasoning to include deductible and remove sum insured/coverage limit references
            if recommendation_reasoning:
                # Remove any sum insured or coverage limit mentions (since it's fixed for all insurers)
                updated_reasoning = recommendation_reasoning
                
                # CRITICAL FIX: Clarify premium comparisons - fix confusing sentence structure
                # Fix pattern: "and a [low/competitive] rate of X, significantly lower than Provider's SAR Y"
                # Replace with: "with a [low/competitive] rate of X, compared to Provider's premium of SAR Y"
                def fix_premium_comparison(match):
                    rate_adj = match.group(2) + ' ' if match.group(2) else ''
                    rate_type = match.group(3)
                    rate_val = match.group(4)
                    provider = match.group(6)
                    premium_val = match.group(7)
                    return f"with {rate_adj}{rate_type} of {rate_val}, compared to {provider}'s premium of SAR {premium_val}"
                
                updated_reasoning = re.sub(
                    r'\band\s+(a|an)?\s*(low|high|competitive)?\s*(rate|premium)\s+of\s+([^,]+),\s+significantly\s+(lower|higher)\s+than\s+([A-Za-z\s\']+?)\'s\s+SAR\s+([\d,]+\.?\d*)',
                    fix_premium_comparison,
                    updated_reasoning,
                    flags=re.IGNORECASE
                )
                # More general fix: "significantly lower than Provider's SAR X" -> "compared to Provider's premium of SAR X"
                updated_reasoning = re.sub(
                    r'\b(significantly|substantially)\s+(lower|higher)\s+than\s+([A-Za-z\s\']+?)\'s\s+SAR\s+([\d,]+\.?\d*)',
                    r'compared to \3\'s premium of SAR \4',
                    updated_reasoning,
                    flags=re.IGNORECASE
                )
                # Fix: "Provider's SAR X" -> "Provider's premium of SAR X" (when not already "premium of")
                updated_reasoning = re.sub(
                    r'\b([A-Za-z\s\']+?)\'s\s+(?!premium\s+of\s+SAR)SAR\s+([\d,]+\.?\d*)(?=\s|\.|,|$)',
                    r'\1\'s premium of SAR \2',
                    updated_reasoning,
                    flags=re.IGNORECASE
                )
                
                # Remove explicit sum insured and coverage limit mentions
                updated_reasoning = re.sub(r'\b(sum insured|Sum Insured|coverage limit|Coverage Limit|substantial coverage limit|total sum insured)\b[^.]*\.?', '', updated_reasoning, flags=re.IGNORECASE)
                # Remove phrases with large numbers + billion/million (targeted: "offers a high 56 billion" pattern)
                updated_reasoning = re.sub(r'\b(offers?|with|of|has|provides?)\s+(a|an)?\s*(high|total|maximum)?\s*[\d,]+\.?\d*\s*(billion|million)\b', '', updated_reasoning, flags=re.IGNORECASE)
                # Remove standalone large numbers with billion/million (only very large numbers to avoid removing premiums)
                updated_reasoning = re.sub(r'\b[\d,]{4,}\.?\d*\s*(billion|million)\b', '', updated_reasoning, flags=re.IGNORECASE)
                # Remove SAR values with very large numbers (likely sum insured, not premiums)
                updated_reasoning = re.sub(r'\b(SAR|SR)\s*[\d,]{9,}\b', '', updated_reasoning, flags=re.IGNORECASE)
                # Clean up whitespace and punctuation
                updated_reasoning = re.sub(r'\s+', ' ', updated_reasoning).strip()
                updated_reasoning = re.sub(r'\s*,\s*,', ',', updated_reasoning)  # Remove double commas
                updated_reasoning = re.sub(r'\s*\.\s*\.', '.', updated_reasoning)  # Remove double periods
                
                # Add deductible consideration if we have deductible data
                if deductibles:
                    updated_reasoning += " The deductible is competitive (lower is better for client benefit)."
            else:
                updated_reasoning = 'Best balance of coverage, competitive premium, favorable policy terms'
                if deductibles:
                    updated_reasoning += ', and optimal deductible (lower is better)'
                updated_reasoning += '.'
            
            rec_text = f"""
            <b>Recommendation:</b> {recommendation}<br/>
            <b>Rationale:</b> {updated_reasoning}
            """
            story.append(Paragraph(rec_text, self.styles['Highlight']))
            story.append(Spacer(1, 0.1*inch))
        else:
            # Fallback recommendation if none available
            fallback_text = """
            <b>Recommendation:</b> Review all providers carefully based on your specific requirements.<br/>
            <b>Rationale:</b> Consider coverage adequacy, premium competitiveness, and policy terms when making your decision.
            """
            story.append(Paragraph(fallback_text, self.styles['Highlight']))
            story.append(Spacer(1, 0.1*inch))
        
        # Top Providers Comparison - Compact table (fit 8 companies)
        if len(ranking) >= 2:
            story.append(Paragraph("PROVIDER COMPARISON", self.styles['SubsectionHeading']))
            story.append(Spacer(1, 0.08*inch))
            
            alt_data = [["Rank", "Provider", "Score", "Premium (SAR)"]]
            
            for i, provider in enumerate(ranking[:8], 1):  # Top 8 providers to fit on page
                if isinstance(provider, dict):
                    rank = str(i)
                    company = provider.get("company", "N/A")
                    score = f"{provider.get('score', 0):.1f}"
                    premium = f"{provider.get('premium', 0):,.2f}"
                    
                    # Create paragraph for company name with appropriate font size
                    # Font size will be handled by table style based on number of rows
                    company_para = Paragraph(company, self.styles['CustomBodyText'])
                    alt_data.append([rank, company_para, score, premium])
            
            if len(alt_data) > 1:
                available_width = self.page_width - 1.5*inch
                # Optimized column widths to fit up to 8 providers with minimum 9pt font
                num_rows = len(alt_data) - 1  # Exclude header
                # Use minimum 9pt font for readability, adjust padding based on rows
                table_font_size = 9  # Minimum 9pt for readability as required
                if num_rows <= 4:
                    row_padding = 4  # Reduced padding for 2-4 rows
                elif num_rows <= 6:
                    row_padding = 3  # Further reduced for 5-6 rows
                else:  # 7-8 rows
                    row_padding = 2  # Minimal padding for 7-8 rows to fit on page
                
                alt_table = Table(alt_data, colWidths=[
                    available_width * 0.10,  # Rank
                    available_width * 0.40,  # Provider
                    available_width * 0.25,  # Score
                    available_width * 0.25   # Premium
                ])
                alt_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), table_font_size),
                    ('FONTSIZE', (0, 1), (-1, -1), table_font_size),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), row_padding),
                    ('TOPPADDING', (0, 0), (-1, -1), row_padding),
                    ('LEFTPADDING', (0, 0), (-1, -1), 3),  # Minimal side padding
                    ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(alt_table)
                story.append(Spacer(1, 0.08*inch))  # Reduced spacing after table
        
        # Critical Decision Factors - Compact
        story.append(Paragraph("KEY DECISION FACTORS", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        # Build decision factors from analytics
        analytics = comparison_data.get("analytics", {})
        statistics = analytics.get("statistics", {})
        
        factors = []
        
        if statistics.get("average_premium"):
            avg_premium = statistics.get("average_premium", 0)
            factors.append(f"• <b>Cost:</b> Market avg SAR {avg_premium:,.2f}")
        
        # Coverage quality - use data_table rows to get actual benefits count
        if rows:
            benefits_counts = []
            for row in rows:
                if isinstance(row, dict):
                    benefits_val = row.get("benefits") or row.get("benefits_count") or 0
                    if isinstance(benefits_val, list):
                        benefits_counts.append(len(benefits_val))
                    else:
                        benefits_counts.append(int(benefits_val) if benefits_val else 0)
            top_benefits_count = max(benefits_counts) if benefits_counts else 0
            if top_benefits_count > 0:
                factors.append(f"• <b>Coverage:</b> Up to {top_benefits_count} benefits offered")
        
        # Risk factors
        factors.append(f"• <b>Risk:</b> Review exclusions carefully")
        
        # Policy flexibility
        factors.append(f"• <b>Terms:</b> Verify subjectivities & conditions")
        
        # Create compact style for factors
        compact_style = ParagraphStyle('CompactFactors', parent=self.styles['CustomBodyText'], fontSize=9, spaceAfter=3)
        for factor in factors:
            story.append(Paragraph(factor, compact_style))
        
        story.append(Spacer(1, 0.2*inch))
        
        # Add Hakem.ai tagline at the bottom
        tagline_style = ParagraphStyle(
            'HakemTagline',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=HexColor('#4A7C2A'),
            alignment=TA_CENTER,
            spaceAfter=12,
            fontName='Helvetica-Bold'
        )
        story.append(Spacer(1, 0.3*inch))
        story.append(Paragraph("Hakem.ai — Empowering Smarter Decisions with Intelligence", tagline_style))
        
        return story
    
    def _build_coverage_analysis_table(self, comparison_data: Dict[str, Any], story: List) -> None:
        """Build Coverage Analysis Table with all providers, subjectivities and exclusions counts."""
        story.append(Paragraph("Coverage Analysis Table", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        side_by_side = comparison_data.get("side_by_side", {})
        providers_data = side_by_side.get("providers", []) if side_by_side else []
        
        if rows:
            # Table: Provider | Count of Benefits | Count of Subjectivities | Count of Exclusions (Benefits first as key comparative signal)
            coverage_data = [["Provider", "Count of Benefits", "Count of Subjectivities", "Count of Exclusions"]]
            
            for row in rows:  # ALL providers
                if isinstance(row, dict):
                    name = row.get("provider_name") or row.get("provider") or row.get("company") or "N/A"
                    
                    # Get benefits count
                    benefits_val = row.get("benefits") or row.get("benefits_count") or 0
                    if isinstance(benefits_val, list):
                        benefits_count = len(benefits_val)
                    else:
                        benefits_count = int(benefits_val) if benefits_val else 0
                    
                    # Get subjectivities count
                    subj_count = 0
                    for provider in providers_data:
                        if provider.get("name") == name:
                            subjectivities = provider.get("subjectivities", [])
                            subj_count = len(subjectivities) if isinstance(subjectivities, list) else 0
                            break
                    
                    # Get exclusions count
                    excl_count = 0
                    for provider in providers_data:
                        if provider.get("name") == name:
                            exclusions = provider.get("exclusions", [])
                            excl_count = len(exclusions) if isinstance(exclusions, list) else 0
                            break
                    
                    name_para = Paragraph(str(name), self.styles['CustomBodyText'])
                    coverage_data.append([
                        name_para,
                        str(benefits_count),
                        str(subj_count),
                        str(excl_count)
                    ])
            
            if len(coverage_data) > 1:
                available_width = self.page_width - 1.5*inch
                coverage_table = Table(coverage_data, colWidths=[
                    available_width * 0.40,
                    available_width * 0.20,
                    available_width * 0.20,
                    available_width * 0.20
                ])
                coverage_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(coverage_table)
                story.append(Spacer(1, 0.1*inch))
                
                # Add technical recommendation one-liner
                tech_rec_style = ParagraphStyle('TechRec', parent=self.styles['CustomBodyText'], 
                                               fontSize=9, textColor=HexColor('#2D5016'), 
                                               leftIndent=0.2*inch, spaceAfter=6)
                story.append(Paragraph("<b>✓ Technical Recommendation:</b> Select providers with lower subjectivities and exclusions counts for better coverage terms.",
                                     tech_rec_style))
                story.append(Spacer(1, 0.15*inch))
    
    def _build_detailed_data_table_hakim_score(self, comparison_data: Dict[str, Any], story: List) -> None:
        """Build Detailed Data Table ordered by Hakim Score (high to low), showing only Hakem Score."""
        story.append(Paragraph("Detailed Data Table (Ordered by Hakim Score)", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        
        if rows:
            # Sort by Hakim score (descending)
            sorted_rows = sorted(
                [r for r in rows if isinstance(r, dict)],
                key=lambda x: float(x.get("score") or x.get("hakim_score") or 0),
                reverse=True
            )
            
            # Table: Provider | Hakim Score | Premium | Benefits | Rate | Rank (REMOVED Coverage column per requirement)
            data_table_data = [["Provider", "Hakem Score", "Premium (SAR)", "Benefits", "Rate", "Rank"]]
            
            for row in sorted_rows:
                name = row.get("provider_name") or row.get("provider") or row.get("company") or "N/A"
                score = float(row.get("score") or row.get("hakim_score") or 0)
                premium = float(row.get("premium") or row.get("premium_amount") or 0)
                
                # Get benefits count
                benefits_val = row.get("benefits") or row.get("benefits_count") or 0
                if isinstance(benefits_val, list):
                    benefits = len(benefits_val)
                else:
                    benefits = int(benefits_val) if benefits_val else 0
                
                # Add note if benefits count is low (2 or less)
                benefits_display = str(benefits)
                if benefits <= 2:
                    benefits_display += "*"
                
                rate = row.get("rate") or "N/A"
                rank = row.get("rank") or 0
                
                name_para = Paragraph(str(name), self.styles['CustomBodyText'])
                data_table_data.append([
                    name_para,
                    f"{score:.1f}",
                    f"{premium:,.2f}",
                    benefits_display,
                    str(rate),
                    str(rank)
                ])
            
            if len(data_table_data) > 1:
                available_width = self.page_width - 1.5*inch
                detail_table = Table(data_table_data, colWidths=[
                    available_width * 0.30,
                    available_width * 0.15,
                    available_width * 0.20,
                    available_width * 0.15,
                    available_width * 0.10,
                    available_width * 0.10
                ])
                detail_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(detail_table)
                
                # Add note about companies with few benefits
                has_low_benefits = any(
                    (int(row.get("benefits_count") or len(row.get("benefits", [])) or 0) if isinstance(row.get("benefits"), list) else int(row.get("benefits") or 0)) <= 2
                    for row in sorted_rows
                )
                if has_low_benefits:
                    note_style = ParagraphStyle('BenefitsNote', parent=self.styles['CustomBodyText'], 
                                              fontSize=8, textColor=colors.grey, spaceAfter=6, leftIndent=0.2*inch)
                    story.append(Spacer(1, 0.05*inch))
                    story.append(Paragraph(
                        "<i>* Note: Some companies show limited benefits. Please revise insurance company's wording for full benefits under this line of business.</i>",
                        note_style
                    ))
                
                story.append(Spacer(1, 0.15*inch))
    
    def _build_premium_comparison_table(self, comparison_data: Dict[str, Any], story: List) -> None:
        """Build Premium Comparison Table ordered by Premium (low to high)."""
        story.append(Paragraph("Premium Comparison Table (Ordered by Premium)", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        
        if rows:
            # Sort by Premium (ascending - lowest first)
            sorted_rows = sorted(
                [r for r in rows if isinstance(r, dict)],
                key=lambda x: float(x.get("premium") or x.get("premium_amount") or 0)
            )
            
            # Table: Provider | Premium | Hakem Score | Benefits
            premium_data = [["Provider", "Premium (SAR)", "Hakem Score", "Benefits Count"]]
            
            for row in sorted_rows:
                name = row.get("provider_name") or row.get("provider") or row.get("company") or "N/A"
                premium = float(row.get("premium") or row.get("premium_amount") or 0)
                score = float(row.get("score") or row.get("hakim_score") or 0)
                
                # Get benefits count
                benefits_val = row.get("benefits") or row.get("benefits_count") or 0
                if isinstance(benefits_val, list):
                    benefits = len(benefits_val)
                else:
                    benefits = int(benefits_val) if benefits_val else 0
                
                name_para = Paragraph(str(name), self.styles['CustomBodyText'])
                premium_data.append([
                    name_para,
                    f"{premium:,.2f}",
                    f"{score:.1f}",
                    str(benefits)
                ])
            
            if len(premium_data) > 1:
                available_width = self.page_width - 1.5*inch
                premium_table = Table(premium_data, colWidths=[
                    available_width * 0.40,
                    available_width * 0.25,
                    available_width * 0.20,
                    available_width * 0.15
                ])
                premium_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(premium_table)
                story.append(Spacer(1, 0.15*inch))
    
    def _build_summary_statistics_table(self, comparison_data: Dict[str, Any], story: List) -> None:
        """Build Summary Statistics Table using Hakim scores and premiums."""
        story.append(Paragraph("Summary Statistics Table", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        
        if rows:
            # Calculate statistics
            scores = [float(r.get("score") or r.get("hakim_score") or 0) for r in rows if isinstance(r, dict)]
            premiums = [float(r.get("premium") or r.get("premium_amount") or 0) for r in rows if isinstance(r, dict)]
            
            best_score = max(scores) if scores else 0
            worst_score = min(scores) if scores else 0
            avg_score = sum(scores) / len(scores) if scores else 0
            
            highest_premium = max(premiums) if premiums else 0
            lowest_premium = min(premiums) if premiums else 0
            avg_premium = sum(premiums) / len(premiums) if premiums else 0
            
            # Build statistics table
            stats_data = [
                ["Metric", "Hakem Score", "Premium (SAR)"],
                ["Best / Highest", f"{best_score:.1f}", f"{highest_premium:,.2f}"],
                ["Worst / Lowest", f"{worst_score:.1f}", f"{lowest_premium:,.2f}"],
                ["Average", f"{avg_score:.1f}", f"{avg_premium:,.2f}"]
            ]
            
            available_width = self.page_width - 1.5*inch
            stats_table = Table(stats_data, colWidths=[
                available_width * 0.40,
                available_width * 0.30,
                available_width * 0.30
            ])
            stats_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
            ]))
            story.append(stats_table)
            story.append(Spacer(1, 0.15*inch))
    
    def _build_detailed_comparison_factors(self, comparison_data: Dict[str, Any]) -> List:
        """
        ✨ DETAILED ANALYSIS: Granular technical report with recommendations.
        Comprehensive comparison of all factors with technical insights.
        """
        story = []
        
        # Clear section header for Detailed Analysis
        story.append(PageBreak())  # Ensure detailed analysis starts on new page
        detail_title = Paragraph("DETAILED TECHNICAL COMPARISON", self.styles['CustomTitle'])
        story.append(detail_title)
        story.append(Spacer(1, 0.05*inch))  # Reduced spacing
        
        # Extract line of business and total sum insured (sum insured is same for all providers)
        # Extract from multiple sources to ensure consistency - use the first valid value found
        line_of_business = "Property Insurance"  # Default
        total_sum_insured = 0
        extracted_quotes = comparison_data.get("extracted_quotes", [])
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        side_by_side = comparison_data.get("side_by_side", {})
        
        # Helper function to extract numeric value from sum insured field
        def extract_sum_insured_numeric(value):
            """Extract numeric sum insured value from various formats."""
            if not value:
                return 0
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                # Remove common prefixes and extract numbers
                cleaned = re.sub(r'[^\d,.]', '', value.replace(" ", ""))
                numbers = re.findall(r'[\d,]+\.?\d*', cleaned)
                if numbers:
                    # Get the largest number (sum insured is usually the largest value)
                    largest_num = max(numbers, key=lambda x: len(x.replace(",", "").replace(".", "")))
                    try:
                        return float(largest_num.replace(",", ""))
                    except (ValueError, AttributeError):
                        return 0
            return 0
        
        # Priority 1: Try extracted_quotes (most reliable source)
        if extracted_quotes and len(extracted_quotes) > 0:
            first_quote = extracted_quotes[0]
            line_of_business = first_quote.get("policy_type") or first_quote.get("insurance_type") or first_quote.get("line_of_business") or "Property Insurance"
            # Try multiple field names
            for field in ["sum_insured", "sum_insured_total", "coverage_limit", "coverage", "total_sum_insured"]:
                sum_insured_val = first_quote.get(field)
                if sum_insured_val:
                    total_sum_insured = extract_sum_insured_numeric(sum_insured_val)
                    if total_sum_insured > 0:
                        break
        
        # Priority 2: Try data_table rows (fallback)
        if total_sum_insured == 0 and rows:
            for row in rows:
                if isinstance(row, dict):
                    for field in ["sum_insured", "coverage_limit", "coverage", "sum_insured_total"]:
                        coverage_val = row.get(field)
                        if coverage_val:
                            total_sum_insured = extract_sum_insured_numeric(coverage_val)
                            if total_sum_insured > 0:
                                break
                    if total_sum_insured > 0:
                        break
        
        # Store sum insured value for consistent use throughout (used ONLY in header, NOT in rationale)
        _total_sum_insured_value = total_sum_insured
        
        # Add line of business and sum insured information at the top
        intro_info_style = ParagraphStyle(
            'IntroInfo',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=HexColor('#2D5016'),
            spaceAfter=8,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        intro_info = f"<b>Line of Business:</b> {line_of_business}"
        if _total_sum_insured_value > 0:
            intro_info += f" | <b>Total Sum Insured:</b> SAR {_total_sum_insured_value:,.2f}"
        story.append(Paragraph(intro_info, intro_info_style))
        story.append(Spacer(1, 0.15*inch))
        
        # ============================================================================
        # PART 1: TABLES FIRST (as per client requirement)
        # ============================================================================
        
        # 1. Coverage Analysis Table (with subjectivities and exclusions counts)
        self._build_coverage_analysis_table(comparison_data, story)
        
        # 2. Detailed Data Table (ordered by Hakim Score)
        self._build_detailed_data_table_hakim_score(comparison_data, story)
        
        # 3. Premium Comparison Table (ordered by Premium low to high)
        self._build_premium_comparison_table(comparison_data, story)
        
        # 4. Summary Statistics Table
        self._build_summary_statistics_table(comparison_data, story)
        
        story.append(PageBreak())  # Page break after tables
        
        # ============================================================================
        # PART 2: TECHNICAL DETAILS FOLLOW
        # ============================================================================
        
        story.append(Paragraph("TECHNICAL DETAILS & ANALYSIS", self.styles['SectionHeading']))
        story.append(Spacer(1, 0.1*inch))
        
        summary = comparison_data.get("summary", {})
        ranking = summary.get("ranking", [])
        side_by_side = comparison_data.get("side_by_side", {})
        key_differences = comparison_data.get("key_differences", {})
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        
        # 1. Subjectivities Aggregated Table
        story.append(Paragraph("1. Policy Subjectivities (Aggregated)", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        providers_data = side_by_side.get("providers", []) if side_by_side else []
        
        if providers_data:
            # Collect all unique subjectivities
            all_subjectivities = set()
            provider_subj_map = {}
            
            for provider in providers_data:
                provider_name = provider.get("name", "Unknown")
                subjectivities = provider.get("subjectivities", [])
                provider_subj_map[provider_name] = set()
                
                for subj in subjectivities:
                    subj_text = str(subj) if isinstance(subj, str) else subj.get("text", str(subj))
                    if self._is_valid_item_text(subj_text):
                        all_subjectivities.add(subj_text)
                        provider_subj_map[provider_name].add(subj_text)
            
            if all_subjectivities:
                # Helper function to get short company name
                def get_short_name(full_name):
                    """Get short name for company (use common abbreviations or first word)."""
                    short_names = {
                        "Chubb Arabia": "Chubb",
                        "Tawuniya": "Tawuniya",
                        "Liva Insurance": "Liva",
                        "Al Rajhi Takaful": "Al Rajhi",
                        "Gulf Insurance Group": "GIG",
                        "United Cooperative Assurance": "UCA",
                    }
                    # Check exact match first
                    if full_name in short_names:
                        return short_names[full_name]
                    # Check partial match
                    for key, short in short_names.items():
                        if key in full_name or full_name in key:
                            return short
                    # Use first word if name is long
                    words = full_name.split()
                    if len(words) > 2:
                        return words[0]
                    return full_name[:15]  # Truncate if still long
                
                # Build subjectivities table with Y/N or check/cross (improved readability)
                subj_list = sorted(list(all_subjectivities))[:10]  # Limit to top 10
                # Use short names for header
                provider_short_names = [get_short_name(p.get("name", "Provider")) for p in providers_data[:5]]
                # Improved header with better wrapping
                header_cell_style = ParagraphStyle('SubjHeader', parent=self.styles['CustomBodyText'], fontSize=8, fontName='Helvetica-Bold', wordWrap='LTR', leading=10)
                header_row = [Paragraph("Subjectivity", header_cell_style)] + [Paragraph(name, header_cell_style) for name in provider_short_names]
                subj_table_data = [header_row]
                
                # Improved cell style for better text wrapping
                subj_cell_style = ParagraphStyle('SubjCell', parent=self.styles['CustomBodyText'], fontSize=7, wordWrap='LTR', leading=9, leftIndent=0, rightIndent=0)
                check_cell_style = ParagraphStyle('SubjCheck', parent=self.styles['CustomBodyText'], fontSize=9, alignment=TA_CENTER)
                
                for subj in subj_list:
                    # Use full text with proper wrapping instead of truncation
                    row = [Paragraph(subj, subj_cell_style)]
                    for provider in providers_data[:5]:
                        provider_name = provider.get("name", "Unknown")
                        has_subj = subj in provider_subj_map.get(provider_name, set())
                        row.append(Paragraph("✓" if has_subj else "✗", check_cell_style))
                    subj_table_data.append(row)
                
                if len(subj_table_data) > 1:
                    available_width = self.page_width - 1.5*inch
                    num_providers = len(subj_table_data[0]) - 1
                    # Allocate more width to subjectivity column for better readability
                    subj_col_width = available_width * 0.50
                    provider_col_width = (available_width - subj_col_width) / num_providers
                    col_widths = [subj_col_width] + [provider_col_width] * num_providers
                    
                    subj_table = Table(subj_table_data, colWidths=col_widths, repeatRows=0)
                    subj_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Top align for better readability with wrapped text
                        ('WORDWRAP', (0, 0), (-1, -1), True),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 8),
                        ('FONTSIZE', (0, 1), (0, -1), 7),  # Subjectivity text smaller
                        ('FONTSIZE', (1, 0), (-1, -1), 8),  # Header and checks normal size
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('TOPPADDING', (0, 0), (-1, -1), 4),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                        ('LEFTPADDING', (0, 0), (-1, -1), 4),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                    ]))
                    story.append(subj_table)
                    story.append(Spacer(1, 0.15*inch))
        
        # 2. Risk Assessment & Exclusions
        story.append(Paragraph("2. Risk Assessment & Exclusions", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.1*inch))
        
        risk_analysis = """
        Understanding policy exclusions is critical for risk management. The following analysis 
        highlights unique exclusions that may impact coverage in specific scenarios.
        """
        story.append(Paragraph(risk_analysis, self.styles['CustomBodyText']))
        story.append(Spacer(1, 0.1*inch))
        
        # Get providers with exclusion data
        providers_data = side_by_side.get("providers", []) if side_by_side else []
        
        if providers_data:
            for provider in providers_data[:3]:  # Top 3 for detailed analysis
                provider_name = provider.get("name", "Unknown")
                exclusions = provider.get("exclusions", [])
                
                if exclusions:
                    story.append(Paragraph(f"<b>{provider_name}</b>", self.styles['CompanyName']))
                    
                    exclusion_count = 0
                    for exclusion in exclusions:
                        exclusion_text = str(exclusion) if isinstance(exclusion, str) else exclusion.get("text", str(exclusion))
                        if self._is_valid_item_text(exclusion_text):
                            story.append(Paragraph(f"• {exclusion_text}", self.styles['CustomBodyText']))
                            exclusion_count += 1
                    
                    if exclusion_count == 0:
                        story.append(Paragraph("• Standard exclusions apply", self.styles['CustomBodyText']))
                    
                    story.append(Spacer(1, 0.1*inch))
        
        # Technical Recommendation for Risk Assessment
        tech_rec_style = ParagraphStyle('TechRec', parent=self.styles['CustomBodyText'], 
                                       fontSize=9, textColor=HexColor('#2D5016'), 
                                       leftIndent=0.2*inch, spaceAfter=6)
        story.append(Paragraph("<b>✓ Technical Recommendation:</b> Cross-reference exclusions with operational risks. "
                             "Engage legal counsel to review cyber, terrorism, and catastrophe exclusions. "
                             "Consider standalone policies for excluded high-risk areas.",
                             tech_rec_style))
        story.append(Spacer(1, 0.15*inch))
        
        # 3. Benefits per Provider (text only, no tables)
        story.append(Paragraph("3. Benefits Comparison per Provider", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.1*inch))
        
        # Try to get benefits from multiple sources
        benefits_found = False
        
        # First, try from data_table rows
        if rows:
            for row in rows[:5]:  # Top 5 providers
                if isinstance(row, dict):
                    provider_name = row.get("provider_name") or row.get("provider") or row.get("company") or "N/A"
                    
                    # Get benefits list - try multiple keys
                    benefits_list = None
                    if "benefits" in row:
                        benefits_val = row.get("benefits")
                        if isinstance(benefits_val, list):
                            benefits_list = benefits_val
                        elif isinstance(benefits_val, (int, float)):
                            # If it's a number, skip - we need the actual list
                            continue
                    
                    # If not found in benefits, try from extracted_quotes or side_by_side
                    if not benefits_list or len(benefits_list) == 0:
                        # Try to find provider in side_by_side data
                        if side_by_side and side_by_side.get("providers"):
                            for provider in side_by_side.get("providers", []):
                                if provider.get("name") == provider_name or provider.get("company") == provider_name:
                                    benefits_list = provider.get("benefits", [])
                                    if isinstance(benefits_list, list) and len(benefits_list) > 0:
                                        break
                    
                    # If still not found, try from extracted_quotes in comparison_data
                    if (not benefits_list or len(benefits_list) == 0) and comparison_data.get("extracted_quotes"):
                        for quote in comparison_data.get("extracted_quotes", []):
                            quote_company = quote.get("company") or quote.get("insurer_name") or quote.get("provider_name")
                            if quote_company and (quote_company in provider_name or provider_name in quote_company):
                                benefits_list = quote.get("benefits", [])
                                if isinstance(benefits_list, list) and len(benefits_list) > 0:
                                    break
                    
                    # Display benefits if found - LIST ALL BENEFITS (no limit)
                    if benefits_list and isinstance(benefits_list, list) and len(benefits_list) > 0:
                        benefits_found = True
                        story.append(Paragraph(f"<b>{provider_name}</b>", self.styles['CompanyName']))
                        story.append(Spacer(1, 0.05*inch))
                        
                        # Display ALL benefits - no truncation
                        benefits_text = []
                        for benefit in benefits_list:  # Show ALL benefits, no limit
                            benefit_text = str(benefit) if isinstance(benefit, str) else benefit.get("text", str(benefit))
                            if self._is_valid_item_text(benefit_text):
                                benefits_text.append(f"• {benefit_text}")
                        
                        if benefits_text:
                            # Create a compact paragraph style for benefits
                            benefits_style = ParagraphStyle(
                                'BenefitsText',
                                parent=self.styles['CustomBodyText'],
                                fontSize=8,
                                spaceAfter=3,
                                leftIndent=0.2*inch,
                                bulletIndent=0.2*inch
                            )
                            
                            # Display all benefits
                            for benefit_item in benefits_text:
                                story.append(Paragraph(benefit_item, benefits_style))
                            
                            # Show total count at the end
                            total_benefits = len(benefits_text)
                            story.append(Paragraph(
                                f"<i>(Total: {total_benefits} benefits)</i>",
                                ParagraphStyle('TotalBenefits', parent=self.styles['CustomBodyText'], 
                                              fontSize=7, textColor=colors.grey, leftIndent=0.2*inch, spaceBefore=4)
                            ))
                        else:
                            story.append(Paragraph("• Standard benefits apply", self.styles['CustomBodyText']))
                        
                        story.append(Spacer(1, 0.1*inch))
        
        # If no benefits found from rows, try side_by_side providers directly
        if not benefits_found and side_by_side and side_by_side.get("providers"):
            for provider in side_by_side.get("providers", [])[:5]:
                provider_name = provider.get("name") or provider.get("company") or "Unknown"
                benefits_list = provider.get("benefits", [])
                
                if benefits_list and isinstance(benefits_list, list) and len(benefits_list) > 0:
                    benefits_found = True
                    story.append(Paragraph(f"<b>{provider_name}</b>", self.styles['CompanyName']))
                    story.append(Spacer(1, 0.05*inch))
                    
                    # Display ALL benefits - no truncation
                    benefits_text = []
                    for benefit in benefits_list:  # Show ALL benefits, no limit
                        benefit_text = str(benefit) if isinstance(benefit, str) else benefit.get("text", str(benefit))
                        if self._is_valid_item_text(benefit_text):
                            benefits_text.append(f"• {benefit_text}")
                    
                    if benefits_text:
                        benefits_style = ParagraphStyle(
                            'BenefitsText',
                            parent=self.styles['CustomBodyText'],
                            fontSize=8,
                            spaceAfter=3,
                            leftIndent=0.2*inch,
                            bulletIndent=0.2*inch
                        )
                        
                        # Display all benefits
                        for benefit_item in benefits_text:
                            story.append(Paragraph(benefit_item, benefits_style))
                        
                        # Show total count at the end
                        total_benefits = len(benefits_text)
                        story.append(Paragraph(
                            f"<i>(Total: {total_benefits} benefits)</i>",
                            ParagraphStyle('TotalBenefits', parent=self.styles['CustomBodyText'], 
                                          fontSize=7, textColor=colors.grey, leftIndent=0.2*inch, spaceBefore=4)
                        ))
                    
                    story.append(Spacer(1, 0.1*inch))
        
        # If still no benefits found, show a message
        if not benefits_found:
            story.append(Paragraph(
                "Benefits information is being processed. Please refer to the detailed data table for benefit counts.",
                self.styles['CustomBodyText']
            ))
        
        story.append(Spacer(1, 0.15*inch))
        
        # Final Technical Recommendation (moved here, removed Key Insights section)
        story.append(Paragraph("Final Technical Recommendation", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        # Get recommendation from comparison data
        key_differences = comparison_data.get("key_differences", {})
        recommendation = key_differences.get("recommendation", "")
        recommendation_reasoning = key_differences.get("recommendation_reasoning", "")
        
        if recommendation and recommendation_reasoning:
            # Remove any sum insured/coverage limit mentions from reasoning (comprehensive removal)
            clean_reasoning = recommendation_reasoning
            
            # CRITICAL FIX: Clarify premium comparisons - fix confusing sentence structure
            # Fix pattern: "and a [low/competitive] rate of X, significantly lower than Provider's SAR Y"
            # Replace with: "with a [low/competitive] rate of X, compared to Provider's premium of SAR Y"
            def fix_premium_comparison_detailed(match):
                rate_adj = match.group(2) + ' ' if match.group(2) else ''
                rate_type = match.group(3)
                rate_val = match.group(4)
                provider = match.group(6)
                premium_val = match.group(7)
                return f"with {rate_adj}{rate_type} of {rate_val}, compared to {provider}'s premium of SAR {premium_val}"
            
            clean_reasoning = re.sub(
                r'\band\s+(a|an)?\s*(low|high|competitive)?\s*(rate|premium)\s+of\s+([^,]+),\s+significantly\s+(lower|higher)\s+than\s+([A-Za-z\s\']+?)\'s\s+SAR\s+([\d,]+\.?\d*)',
                fix_premium_comparison_detailed,
                clean_reasoning,
                flags=re.IGNORECASE
            )
            # More general fix: "significantly lower than Provider's SAR X" -> "compared to Provider's premium of SAR X"
            clean_reasoning = re.sub(
                r'\b(significantly|substantially)\s+(lower|higher)\s+than\s+([A-Za-z\s\']+?)\'s\s+SAR\s+([\d,]+\.?\d*)',
                r'compared to \3\'s premium of SAR \4',
                clean_reasoning,
                flags=re.IGNORECASE
            )
            # Fix: "Provider's SAR X" -> "Provider's premium of SAR X" (when not already "premium of")
            clean_reasoning = re.sub(
                r'\b([A-Za-z\s\']+?)\'s\s+(?!premium\s+of\s+SAR)SAR\s+([\d,]+\.?\d*)(?=\s|\.|,|$)',
                r'\1\'s premium of SAR \2',
                clean_reasoning,
                flags=re.IGNORECASE
            )
            
            # Remove explicit sum insured and coverage limit mentions
            clean_reasoning = re.sub(r'\b(sum insured|Sum Insured|coverage limit|Coverage Limit|substantial coverage limit|total sum insured)\b[^.]*\.?', '', clean_reasoning, flags=re.IGNORECASE)
            # Remove phrases with large numbers + billion/million (e.g., "offers a high 56 billion")
            clean_reasoning = re.sub(r'\b(offers?|with|of|has)\s+(a|an)?\s*(high|total|maximum)?\s*[\d,]+\.?\d*\s*(billion|million)\b', '', clean_reasoning, flags=re.IGNORECASE)
            # Remove standalone large numbers with billion/million (only very large numbers to avoid removing premiums)
            clean_reasoning = re.sub(r'\b[\d,]{4,}\.?\d*\s*(billion|million)\b', '', clean_reasoning, flags=re.IGNORECASE)
            # Remove SAR values with very large numbers (likely sum insured)
            clean_reasoning = re.sub(r'\b(SAR|SR)\s*[\d,]{9,}\b', '', clean_reasoning, flags=re.IGNORECASE)
            # Clean up whitespace and punctuation
            clean_reasoning = re.sub(r'\s+', ' ', clean_reasoning).strip()
            clean_reasoning = re.sub(r'\s*,\s*,', ',', clean_reasoning)  # Remove double commas
            clean_reasoning = re.sub(r'\s*\.\s*\.', '.', clean_reasoning)  # Remove double periods
            
            final_rec_text = f"""
            Based on comprehensive analysis of coverage quality, pricing competitiveness, policy terms, and 
            risk assessment, we recommend <b>{recommendation}</b>. {clean_reasoning}
            
            This recommendation considers the optimal balance between coverage adequacy, premium efficiency, 
            and favorable policy conditions including subjectivities, exclusions, and deductibles.
            """
        else:
            # Fallback recommendation based on ranking
            summary = comparison_data.get("summary", {})
            ranking = summary.get("ranking", [])
            if ranking and len(ranking) > 0:
                top_provider = ranking[0].get("company", "the top-ranked provider")
                final_rec_text = f"""
                Based on comprehensive analysis of coverage quality, pricing competitiveness, policy terms, and 
                risk assessment, we recommend <b>{top_provider}</b> as the optimal choice. This provider offers 
                the best balance between coverage adequacy, premium efficiency, and favorable policy conditions 
                including subjectivities, exclusions, and deductibles.
                """
            else:
                final_rec_text = """
                Based on comprehensive analysis, we recommend selecting the provider that offers the best balance 
                between coverage adequacy, premium efficiency, and favorable policy conditions including subjectivities, 
                exclusions, and deductibles. Please review the detailed comparison tables above for specific metrics.
                """
        
        final_rec_style = ParagraphStyle(
            'FinalRecommendation',
            parent=self.styles['CustomBodyText'],
            fontSize=10,
            textColor=HexColor('#2D5016'),
            spaceAfter=8,
            alignment=TA_JUSTIFY,
            leftIndent=0.2*inch,
            rightIndent=0.2*inch
        )
        story.append(Paragraph(final_rec_text, final_rec_style))
        story.append(Spacer(1, 0.15*inch))
        
        return story
    
    def _build_summary_section(self, comparison_data: Dict[str, Any]) -> List:
        """Build summary section with ranking and overview."""
        story = []
        
        # Section title
        title = Paragraph("Executive Summary", self.styles['SectionHeading'])
        story.append(title)
        story.append(Spacer(1, 0.15*inch))
        
        summary = comparison_data.get("summary", {})
        if not summary:
            story.append(Paragraph("No summary data available.", self.styles['CustomBodyText']))
            return story
        
        # Analysis summary
        if summary.get("analysis_summary"):
            summary_text = Paragraph(
                f"<b>Overview:</b><br/>{summary.get('analysis_summary', '')}",
                self.styles['CustomBodyText']
            )
            story.append(summary_text)
            story.append(Spacer(1, 0.15*inch))
        
        # Ranking table
        ranking = summary.get("ranking", [])
        if ranking and len(ranking) > 0:
            story.append(Paragraph("Provider Rankings", self.styles['SubsectionHeading']))
            
            # Prepare ranking table data - improved format (NO HAKIM SCORE)
            table_data = [["Rank", "Provider", "Score", "Premium (SAR)", "Rate"]]
            
            for item in ranking[:10]:  # Limit to top 10
                if not isinstance(item, dict):
                    continue  # Skip invalid items
                
                try:
                    rank = str(item.get("rank", ""))
                    company = item.get("company", "N/A")
                    score_val = item.get('score', 0)
                    score = f"{float(score_val):.1f}" if score_val else "0.0"
                    premium_val = item.get('premium', 0)
                    premium = f"{float(premium_val):,.2f}" if premium_val else "0.00"
                    rate = item.get("rate", "N/A")
                    
                    # CRITICAL FIX: Wrap provider name in Paragraph for text wrapping
                    company_paragraph = Paragraph(company, self.styles['CustomBodyText'])
                    table_data.append([rank, company_paragraph, score, premium, rate])
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️  Error processing ranking item: {e}")
                    continue
            
            # Only create table if we have data rows (more than just header)
            if len(table_data) > 1:
                # Adjusted column widths to fit margins (removed Hakim Score column)
                ranking_table = Table(table_data, colWidths=[0.6*inch, 2.2*inch, 0.9*inch, 1.3*inch, 0.9*inch])
                
                # Build table style
                table_style = [
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Provider name left-aligned
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),  # Other columns center
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),  # CRITICAL FIX: Enable word wrap
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                    ('TOPPADDING', (0, 0), (-1, 0), 10),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ]
                
                # Only add ROWBACKGROUNDS if we have data rows (more than just header)
                if len(table_data) > 2:
                    table_style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]))
                
                ranking_table.setStyle(TableStyle(table_style))
                story.append(ranking_table)
                story.append(Spacer(1, 0.15*inch))
            else:
                story.append(Paragraph("No ranking data available.", self.styles['CustomBodyText']))
        
        # Best overall and best value
        if summary.get("best_overall") or summary.get("best_value"):
            recommendations = []
            if summary.get("best_overall"):
                recommendations.append(f"<b>Best Overall:</b> {summary.get('best_overall', 'N/A')}")
            if summary.get("best_value"):
                recommendations.append(f"<b>Best Value:</b> {summary.get('best_value', 'N/A')}")
            
            if recommendations:
                rec_text = Paragraph("<br/>".join(recommendations), self.styles['CustomBodyText'])
                story.append(rec_text)
        
        return story
    
    def _build_key_differences_section(self, comparison_data: Dict[str, Any]) -> List:
        """Build key differences section with all warranties, exclusions, subjectivities per provider."""
        story = []
        
        title = Paragraph("Key Differences", self.styles['SectionHeading'])
        story.append(title)
        story.append(Spacer(1, 0.15*inch))
        
        key_differences = comparison_data.get("key_differences", {})
        side_by_side = comparison_data.get("side_by_side", {})
        
        # Recommendation with detailed reasoning
        if key_differences.get("recommendation"):
            rec_text = Paragraph(
                f"<b>Recommendation:</b> {key_differences.get('recommendation', '')}",
                self.styles['Highlight']
            )
            story.append(rec_text)
            story.append(Spacer(1, 0.1*inch))
            
            # Add detailed reasoning if available
            if key_differences.get("recommendation_reasoning"):
                reasoning_text = Paragraph(
                    f"<b>Why this is the best choice:</b> {key_differences.get('recommendation_reasoning', '')}",
                    self.styles['CustomBodyText']
                )
                story.append(reasoning_text)
            
            story.append(Spacer(1, 0.2*inch))
        
        # Get provider details from side_by_side or summary
        providers_data = []
        if side_by_side and side_by_side.get("providers"):
            providers_data = side_by_side.get("providers", [])
        elif comparison_data.get("summary") and comparison_data.get("summary", {}).get("ranking"):
            # Extract from ranking
            ranking = comparison_data.get("summary", {}).get("ranking", [])
            for item in ranking:
                if isinstance(item, dict):
                    providers_data.append({
                        "name": item.get("company", "Unknown"),
                        "warranties": item.get("warranties", []),
                        "exclusions": item.get("exclusions", []),
                        "subjectivities": item.get("subjectivities", [])
                    })
        
        # Show unique warranties, exclusions, subjectivities per provider
        if providers_data:
            for provider in providers_data:
                provider_name = provider.get("name", "Unknown")
                
                # Provider header
                provider_header = Paragraph(f"<b>{provider_name}</b>", self.styles['SubsectionHeading'])
                story.append(provider_header)
                story.append(Spacer(1, 0.1*inch))
                
                # Unique Warranties - SHOW ALL (filtered for quality)
                warranties = provider.get("warranties", []) or provider.get("unique_warranties", [])
                if warranties:
                    story.append(Paragraph("<b>Unique Warranties:</b>", self.styles['CustomBodyText']))
                    displayed_count = 0
                    for warranty in warranties:  # NO LIMIT - show all valid items
                        warranty_text = str(warranty) if isinstance(warranty, str) else warranty.get("text", str(warranty))
                        # CRITICAL FIX: Skip truncated, incomplete, or placeholder text
                        if self._is_valid_item_text(warranty_text):
                            story.append(Paragraph(f"• {warranty_text}", self.styles['CustomBodyText']))
                            displayed_count += 1
                    if displayed_count == 0:
                        story.append(Paragraph("• No unique warranties identified", self.styles['CustomBodyText']))
                    story.append(Spacer(1, 0.1*inch))
                
                # Unique Exclusions - SHOW ALL (filtered for quality)
                exclusions = provider.get("exclusions", []) or provider.get("unique_exclusions", [])
                if exclusions:
                    story.append(Paragraph("<b>Unique Exclusions:</b>", self.styles['CustomBodyText']))
                    displayed_count = 0
                    for exclusion in exclusions:  # NO LIMIT - show all valid items
                        exclusion_text = str(exclusion) if isinstance(exclusion, str) else exclusion.get("text", str(exclusion))
                        # CRITICAL FIX: Skip truncated, incomplete, or placeholder text
                        if self._is_valid_item_text(exclusion_text):
                            story.append(Paragraph(f"• {exclusion_text}", self.styles['CustomBodyText']))
                            displayed_count += 1
                    if displayed_count == 0:
                        story.append(Paragraph("• No unique exclusions identified", self.styles['CustomBodyText']))
                    story.append(Spacer(1, 0.1*inch))
                
                # Unique Subjectivities - SHOW ALL (filtered for quality)
                subjectivities = provider.get("subjectivities", []) or provider.get("unique_subjectivities", [])
                if subjectivities:
                    story.append(Paragraph("<b>Unique Subjectivities:</b>", self.styles['CustomBodyText']))
                    displayed_count = 0
                    for subjectivity in subjectivities:  # NO LIMIT - show all valid items
                        subj_text = str(subjectivity) if isinstance(subjectivity, str) else subjectivity.get("text", str(subjectivity))
                        # CRITICAL FIX: Skip truncated, incomplete, or placeholder text
                        if self._is_valid_item_text(subj_text):
                            story.append(Paragraph(f"• {subj_text}", self.styles['CustomBodyText']))
                            displayed_count += 1
                    if displayed_count == 0:
                        story.append(Paragraph("• No unique subjectivities identified", self.styles['CustomBodyText']))
                    story.append(Spacer(1, 0.15*inch))
        
        # Price differences table (if available)
        differences = key_differences.get("differences", [])
        if differences:
            story.append(Paragraph("Price Differences", self.styles['SubsectionHeading']))
            story.append(Spacer(1, 0.1*inch))
            
            # CRITICAL FIX: Use actual company names from first difference, not hardcoded "Provider 1/2"
            first_diff = differences[0] if differences else {}
            header1 = first_diff.get("provider1", "Insurer 1")
            header2 = first_diff.get("provider2", "Insurer 2")
            
            # CRITICAL FIX: Create dedicated header style for text wrapping
            from reportlab.lib.styles import ParagraphStyle
            header_style = ParagraphStyle(
                'PriceDiffHeader',
                parent=self.styles['CustomBodyText'],
                fontSize=8,
                leading=10,
                textColor=colors.whitesmoke,
                fontName='Helvetica-Bold',
                alignment=0,  # Left alignment
                wordWrap='LTR',
                splitLongWords=True
            )
            
            # CRITICAL FIX: Wrap ALL header cells in Paragraphs to enable text wrapping
            header1_para = Paragraph(header1, header_style)
            header2_para = Paragraph(header2, header_style)
            header3_para = Paragraph("Price Difference (SAR)", header_style)
            header4_para = Paragraph("Difference %", header_style)
            
            table_data = [[header1_para, header2_para, header3_para, header4_para]]
            
            # Create body cell style
            body_style = ParagraphStyle(
                'PriceDiffBody',
                parent=self.styles['CustomBodyText'],
                fontSize=7,
                leading=9,
                alignment=0,  # Left alignment
                wordWrap='LTR',
                splitLongWords=True
            )
            
            for diff in differences[:10]:  # Limit to 10 differences
                provider1 = diff.get("provider1", "N/A")
                provider2 = diff.get("provider2", "N/A")
                price_diff = f"{diff.get('price_difference', 0):,.2f}"
                diff_pct = f"{diff.get('price_difference_percentage', 0):.2f}%"  # Direct % symbol
                
                # CRITICAL FIX: Wrap provider names in Paragraph for text wrapping
                provider1_para = Paragraph(provider1, body_style)
                provider2_para = Paragraph(provider2, body_style)
                
                table_data.append([provider1_para, provider2_para, price_diff, diff_pct])
            
            # Ensure table fits within margins
            available_width = self.page_width - 1.5*inch
            col_widths = [available_width * 0.3, available_width * 0.3, available_width * 0.2, available_width * 0.2]
            
            # CRITICAL FIX: Set rowHeights=None for dynamic row expansion
            diff_table = Table(table_data, colWidths=col_widths, rowHeights=None)
            diff_style = [
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (1, -1), 'LEFT'),  # Provider columns left-aligned
                ('ALIGN', (2, 0), (-1, -1), 'CENTER'),  # Price/% columns center
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # CRITICAL FIX: TOP alignment prevents overlap
                ('WORDWRAP', (0, 0), (-1, -1), True),  # CRITICAL FIX: Enable word wrap
                # FONTNAME and FONTSIZE removed for header since it's now in Paragraph style
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),  # Increased padding for header
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 6),  # Padding for data rows
                ('TOPPADDING', (0, 1), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ]
            if len(table_data) > 2:
                diff_style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]))
            diff_table.setStyle(TableStyle(diff_style))
            story.append(diff_table)
        
        return story
    
    def _build_data_table_section(self, comparison_data: Dict[str, Any]) -> List:
        """Build data table section matching frontend UI format EXACTLY."""
        story = []
        
        title = Paragraph("Detailed Data Table", self.styles['SectionHeading'])
        story.append(title)
        story.append(Spacer(1, 0.15*inch))
        
        data_table = comparison_data.get("data_table", {})
        if not data_table:
            story.append(Paragraph("No data table available.", self.styles['CustomBodyText']))
            return story
        
        rows = data_table.get("rows", [])
        if not rows:
            story.append(Paragraph("No data rows available.", self.styles['CustomBodyText']))
            return story
        
        # EXACT UI COLUMNS (in order): Provider name, Score, Premium, Rate, Coverage, Benefits, Exclusions, Warranties, Rank
        ui_columns = [
            ("provider_name", "Provider name"),
            ("score", "Score"),
            ("premium", "Premium"),
            ("rate", "Rate"),
            ("coverage", "Coverage"),
            ("benefits", "Benefits"),
            ("exclusions", "Exclusions"),
            ("warranties", "Warranties"),
            ("rank", "Rank")
        ]
        
        # Build table data with EXACT UI format
        table_data = []
        
        # Header row - EXACT UI column names wrapped in Paragraph for proper spacing
        from reportlab.lib.styles import ParagraphStyle
        header_style = ParagraphStyle(
            'TableHeader',
            parent=self.styles['CustomBodyText'],
            fontSize=8,
            leading=10,
            alignment=1,  # Center alignment
            textColor=colors.white,
            fontName='Helvetica-Bold',
            wordWrap='LTR'
        )
        
        # CRITICAL FIX: Wrap each header in Paragraph to prevent text overlap
        header_row = [Paragraph(col_display, header_style) for _, col_display in ui_columns]
        table_data.append(header_row)
        
        # Data rows - extract values in UI order
        for row_idx, row in enumerate(rows):
            try:
                if not isinstance(row, dict):
                    continue
                
                row_data = []
                for col_key, _ in ui_columns:
                    # Try multiple key variations
                    value = None
                    
                    # Provider name
                    if col_key == "provider_name":
                        value = row.get("provider_name") or row.get("provider") or row.get("company") or row.get("company_name") or "N/A"
                    
                    # Score
                    elif col_key == "score":
                        value = row.get("score") or row.get("weighted_score") or 0
                    
                    # Premium
                    elif col_key == "premium":
                        value = row.get("premium") or row.get("premium_amount") or 0
                    
                    # Rate
                    elif col_key == "rate":
                        value = row.get("rate") or "N/A"
                    
                    # Coverage
                    elif col_key == "coverage":
                        coverage_val = row.get("coverage") or row.get("coverage_limit") or 0
                        # If it's a string like "SR 1,564,652,306", extract the number
                        if isinstance(coverage_val, str):
                            # Try to extract numeric value from string
                            numbers = re.findall(r'[\d,]+', coverage_val.replace(" ", ""))
                            if numbers:
                                # Take the largest number found
                                largest = max(numbers, key=lambda x: len(x.replace(",", "")))
                                value = largest.replace(",", "")
                            else:
                                value = 0
                        else:
                            value = coverage_val
                    
                    # Benefits (COUNT ONLY, not list)
                    elif col_key == "benefits":
                        # Get count, not the list
                        if "benefits_count" in row:
                            value = row.get("benefits_count") or 0
                        elif "benefits" in row:
                            benefits_val = row.get("benefits")
                            if isinstance(benefits_val, (int, float)):
                                value = int(benefits_val)
                            elif isinstance(benefits_val, list):
                                value = len(benefits_val)
                            else:
                                value = 0
                        else:
                            value = 0
                    
                    # Exclusions (COUNT ONLY, not list)
                    elif col_key == "exclusions":
                        # Get count, not the list
                        if "exclusions_count" in row:
                            value = row.get("exclusions_count") or 0
                        elif "exclusions" in row:
                            exclusions_val = row.get("exclusions")
                            if isinstance(exclusions_val, (int, float)):
                                value = int(exclusions_val)
                            elif isinstance(exclusions_val, list):
                                value = len(exclusions_val)
                            else:
                                value = 0
                        else:
                            value = 0
                    
                    # Warranties (COUNT ONLY, not list)
                    elif col_key == "warranties":
                        # Get count, not the list
                        if "warranties_count" in row:
                            value = row.get("warranties_count") or 0
                        elif "warranties" in row:
                            warranties_val = row.get("warranties")
                            if isinstance(warranties_val, (int, float)):
                                value = int(warranties_val)
                            elif isinstance(warranties_val, list):
                                value = len(warranties_val)
                            else:
                                value = 0
                        else:
                            value = 0
                    
                    # Rank
                    elif col_key == "rank":
                        value = row.get("rank") or row.get("ranking") or 0
                    
                    # Format the value for display
                    formatted_value = self._format_ui_table_cell(value, col_key)
                    row_data.append(formatted_value)
                
                if len(row_data) == len(ui_columns):
                    table_data.append(row_data)
            except Exception as e:
                logger.warning(f"⚠️  Error processing row {row_idx}: {e}")
                continue
        
        if len(table_data) < 2:
            story.append(Paragraph("No valid data rows available.", self.styles['CustomBodyText']))
            return story
        
        # Calculate column widths to fit UI proportions - CRITICAL FIX: Wrap text for long provider names
        available_width = self.page_width - 1.5*inch  # Conservative margins
        col_widths = [
            available_width * 0.20,  # Provider name (20%) - will wrap if needed
            available_width * 0.10,  # Score (10%)
            available_width * 0.12,  # Premium (12%)
            available_width * 0.10,  # Rate (10%)
            available_width * 0.15,  # Coverage (15%)
            available_width * 0.08,  # Benefits (8%)
            available_width * 0.08,  # Exclusions (8%)
            available_width * 0.08,  # Warranties (8%)
            available_width * 0.09,  # Rank (9%)
        ]
        
        # CRITICAL FIX: Create smaller paragraph style for table cells
        from reportlab.lib.styles import ParagraphStyle
        table_cell_style = ParagraphStyle(
            'TableCell',
            parent=self.styles['CustomBodyText'],
            fontSize=8,
            leading=10,  # Line height
            alignment=0,  # Left alignment
            wordWrap='LTR',
            splitLongWords=True
        )
        
        # CRITICAL FIX: Wrap provider names in Paragraph for text wrapping
        # Also wrap long numeric values to prevent cell overflow
        for i, row in enumerate(table_data):
            if i > 0 and len(row) > 0:  # Skip header row
                # Wrap provider name (first column) in Paragraph with smaller font
                provider_name = str(row[0])
                table_data[i][0] = Paragraph(provider_name, table_cell_style)
                
                # Wrap coverage values if they're very long numbers
                if len(row) > 4:  # Coverage column
                    coverage_val = str(row[4])
                    if len(coverage_val) > 10:  # Long number
                        table_data[i][4] = Paragraph(coverage_val, table_cell_style)
        
        # CRITICAL FIX: Create table with dynamic row heights and explicit spacing
        data_table_obj = Table(table_data, colWidths=col_widths, repeatRows=1, rowHeights=None, spaceBefore=0, spaceAfter=0)
        
        # EXACT UI COLORS AND STYLING WITH ENHANCED VERTICAL SPACING
        data_style = [
            # Header row - Dark green background (#4A7C2A) with WHITE text
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),  # WHITE, not whitesmoke
            # FONTNAME and FONTSIZE removed here since headers are now Paragraphs with their own style
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),  # Increased padding for header
            ('TOPPADDING', (0, 0), (-1, 0), 10),  # Increased padding for header
            
            # Alignment: Provider name LEFT, others CENTER
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Provider name left-aligned
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),  # All other columns center-aligned
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # CRITICAL FIX: TOP alignment prevents overlap
            
            # CRITICAL FIX: Enable word wrap for all cells
            ('WORDWRAP', (0, 0), (-1, -1), True),
            
            # Data rows - White and light grey alternating
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),  # Default white
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),  # Smaller font for data (8pt instead of 9pt)
            
            # Grid lines
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            
            # CRITICAL FIX: Increased vertical padding to prevent overlap
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 1), (-1, -1), 10),  # Increased from 6 to 10
            ('BOTTOMPADDING', (0, 1), (-1, -1), 10),  # Increased from 6 to 10
        ]
        
        # Add alternating row backgrounds (white and light grey #F5F5F5)
        if len(table_data) > 2:
            data_style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]))
        
        data_table_obj.setStyle(TableStyle(data_style))
        story.append(data_table_obj)
        
        return story
    
    def _is_valid_item_text(self, text: str) -> bool:
        """
        Validate that item text is complete and not truncated.
        Returns True if text is valid for display, False if truncated/incomplete.
        """
        if not text or not str(text).strip():
            return False
        
        text_str = str(text).strip()
        
        # Skip if too short (likely incomplete)
        if len(text_str) < 5:
            return False
        
        # CRITICAL FIX: Skip truncated items with ellipsis markers
        truncation_markers = [
            '...',
            '... all',
            '... and',
            '…',  # Unicode ellipsis
            'all requirements',
            'all conditions',
            'all exclusions',
            'all warranties'
        ]
        
        text_lower = text_str.lower()
        
        # Check if text starts with ellipsis (incomplete beginning)
        if text_str.startswith('...') or text_str.startswith('…'):
            return False
        
        # Check if text is ONLY a truncation phrase
        if text_lower in ['all requirements', 'all conditions', 'all exclusions', 'all warranties']:
            return False
        
        # Check if text starts with "... " followed by truncation phrase
        for marker in truncation_markers:
            if text_lower.startswith(marker):
                return False
        
        # If text is very short and contains ellipsis, skip it
        if len(text_str) < 30 and '...' in text_str:
            return False
        
        return True
    
    def _format_ui_table_cell(self, value: Any, column_key: str) -> str:
        """Format table cell values to match UI exactly."""
        if value is None:
            return "N/A"
        
        # Provider name - return as-is (string)
        if column_key == "provider_name":
            return str(value) if value else "N/A"
        
        # Score - format as decimal with 1 decimal place and % sign (UI shows "77.0%")
        elif column_key == "score":
            try:
                score_val = float(value) if value else 0
                return f"{score_val:.1f}%"  # Direct % symbol
            except (ValueError, TypeError):
                return "0.0%"
        
        # Premium - format as number without SAR prefix (UI shows just numbers with commas)
        elif column_key == "premium":
            try:
                premium_val = float(value) if value else 0
                # Format with commas, no decimal places (UI shows integers)
                return f"{premium_val:,.0f}"
            except (ValueError, TypeError):
                return "0"
        
        # Rate - format as percentage or per mille (handle FLAT Premium case)
        elif column_key == "rate":
            if isinstance(value, str):
                # CRITICAL FIX: Handle "FLAT Premium" case - don't add % sign
                if 'FLAT' in value.upper() or 'PREMIUM' in value.upper():
                    return value  # Return as-is for FLAT Premium
                # If it already has % or ‰, return as-is
                if '%' in value or '‰' in value:
                    return value
                # Try to parse as number if it's a numeric string
                try:
                    rate_val = float(value.replace('%', '').replace('‰', '').strip())
                    if rate_val < 1:
                        return f"{rate_val:.2f}\u2030"  # Unicode for ‰
                    else:
                        return f"{rate_val:.2f}%"  # Direct % symbol
                except:
                    return value  # Return original if can't parse
            try:
                rate_val = float(value) if value else 0
                if rate_val < 1:
                    return f"{rate_val:.2f}\u2030"  # Unicode for ‰
                else:
                    return f"{rate_val:.2f}%"  # Direct % symbol
            except (ValueError, TypeError):
                return "N/A"
        
        # Coverage - format as number with commas
        elif column_key == "coverage":
            try:
                # If it's already a string with formatting, return as-is
                if isinstance(value, str):
                    return value
                coverage_val = float(value) if value else 0
                return f"{coverage_val:,.0f}".replace(",", ",")
            except (ValueError, TypeError):
                return "0"
        
        # Benefits, Exclusions, Warranties - show count only (integer)
        elif column_key in ["benefits", "exclusions", "warranties"]:
            try:
                count_val = int(value) if value else 0
                return str(count_val)
            except (ValueError, TypeError):
                return "0"
        
        # Rank - show integer
        elif column_key == "rank":
            try:
                rank_val = int(value) if value else 0
                return str(rank_val)
            except (ValueError, TypeError):
                return "0"
        
        # Default - return as string
        return str(value) if value else "N/A"
    
    def _format_table_cell_value(self, value: Any, column_name: str) -> str:
        """Format table cell values with proper formatting (NO TRUNCATION - use Paragraph wrapping)."""
        if value is None:
            return "N/A"
        
        col_lower = str(column_name).lower()
        
        # Format numbers
        if isinstance(value, (int, float)):
            if 'premium' in col_lower:
                return f"SAR {value:,.2f}"
            elif 'coverage' in col_lower:
                return f"{value:,.0f}"
            elif 'rate' in col_lower:
                if isinstance(value, float) and value < 1:
                    return f"{value:.2f}\u2030"  # Unicode for ‰
                return f"{value:.2f}%"  # Direct % symbol
            elif 'score' in col_lower:
                return f"{value:.1f}"
            else:
                return f"{value:,.0f}"
        
        # CRITICAL FIX: Return full string WITHOUT truncation
        # ReportLab Paragraph will handle text wrapping automatically
        return str(value)
    
    def _build_side_by_side_section(self, comparison_data: Dict[str, Any]) -> List:
        """Build side-by-side comparison section."""
        story = []
        
        title = Paragraph("Side-by-Side Comparison", self.styles['SectionHeading'])
        story.append(title)
        story.append(Spacer(1, 0.15*inch))
        
        side_by_side = comparison_data.get("side_by_side", {})
        if not side_by_side:
            story.append(Paragraph("No side-by-side data available.", self.styles['CustomBodyText']))
            return story
        
        # Providers list (NO HAKIM SCORE)
        providers = side_by_side.get("providers", [])
        if providers:
            story.append(Paragraph("Providers", self.styles['SubsectionHeading']))
            story.append(Spacer(1, 0.1*inch))
            
            provider_data = [["Provider", "Score", "Premium (SAR)", "Rate"]]
            
            for provider in providers:
                name = provider.get("name", "N/A")
                score = provider.get("score", 0)
                premium = provider.get("premium", 0)
                rate = provider.get("rate", "N/A")
                
                # CRITICAL FIX: Wrap provider name in Paragraph for text wrapping
                name_paragraph = Paragraph(name, self.styles['CustomBodyText'])
                
                provider_data.append([
                    name_paragraph,
                    f"{float(score):.1f}" if score else "N/A",
                    f"{float(premium):,.2f}" if premium else "0.00",
                    str(rate) if rate else "N/A"
                ])
            
            # Ensure table fits margins
            available_width = self.page_width - 1.8*inch
            provider_table = Table(provider_data, colWidths=[
                available_width * 0.4,
                available_width * 0.2,
                available_width * 0.25,
                available_width * 0.15
            ])
            provider_style = [
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Provider name left-aligned
                ('ALIGN', (1, 0), (-1, -1), 'CENTER'),  # Other columns center
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('WORDWRAP', (0, 0), (-1, -1), True),  # CRITICAL FIX: Enable word wrap
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ]
            if len(provider_data) > 2:
                provider_style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]))
            provider_table.setStyle(TableStyle(provider_style))
            story.append(provider_table)
            story.append(Spacer(1, 0.15*inch))
        
        # Comparison matrix
        comparison_matrix = side_by_side.get("comparison_matrix", {})
        if comparison_matrix:
            story.append(Paragraph("Comparison Matrix", self.styles['SubsectionHeading']))
            
            # Premium comparison
            if "premium" in comparison_matrix:
                premium_data = comparison_matrix["premium"]
                if premium_data:
                    table_data = [["Provider", "Premium (SAR)"]]
                    for item in premium_data:
                        provider = item.get("provider", "N/A")
                        premium = item.get("formatted", item.get("value", "N/A"))
                        # CRITICAL FIX: Wrap provider name in Paragraph
                        provider_paragraph = Paragraph(provider, self.styles['CustomBodyText'])
                        table_data.append([provider_paragraph, premium])
                    
                    # Ensure table fits within margins
                    available_width = self.page_width - 1.8*inch
                    premium_table = Table(table_data, colWidths=[available_width * 0.5, available_width * 0.5])
                    premium_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#6B9F3D')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Provider column left
                        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),  # Other columns center
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('WORDWRAP', (0, 0), (-1, -1), True),  # CRITICAL FIX: Enable word wrap
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 8),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                                                ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ]))
                    story.append(premium_table)
                    story.append(Spacer(1, 0.15*inch))
        
        return story
    
    def _build_analytics_section(self, comparison_data: Dict[str, Any]) -> List:
        """Build analytics section with Overall Score Comparison only (removed duplicated tables per client requirement)."""
        story = []
        
        title = Paragraph("Score Comparison", self.styles['SectionHeading'])
        story.append(title)
        story.append(Spacer(1, 0.15*inch))
        
        summary = comparison_data.get("summary", {})
        ranking = summary.get("ranking", []) if summary else []
        
        # Overall Score Comparison (only table kept per client requirement)
        if ranking:
            story.append(Paragraph("Overall Score Comparison", self.styles['SubsectionHeading']))
            story.append(Spacer(1, 0.1*inch))
            
            table_data = [["Provider", "Score", "Rank"]]
            for item in ranking[:10]:
                if isinstance(item, dict):
                    provider = item.get("company", "N/A")
                    score = item.get("score", 0)
                    rank = item.get("rank", "N/A")
                    # CRITICAL FIX: Wrap provider name in Paragraph
                    provider_paragraph = Paragraph(provider, self.styles['CustomBodyText'])
                    table_data.append([provider_paragraph, f"{score:.2f}", str(rank)])
            
            if len(table_data) > 1:
                available_width = self.page_width - 1.8*inch
                score_table = Table(table_data, colWidths=[available_width * 0.5, available_width * 0.3, available_width * 0.2])
                score_style = [
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#6B9F3D')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Provider column left
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),  # Other columns center
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),  # CRITICAL FIX: Enable word wrap
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ]
                if len(table_data) > 2:
                    score_style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]))
                score_table.setStyle(TableStyle(score_style))
                story.append(score_table)
                story.append(Spacer(1, 0.2*inch))
        
        # Key Insights section removed per client requirement - Final Technical Recommendation is in _build_detailed_comparison_factors
        
        return story


# Global singleton instance
pdf_generator_service = PDFGeneratorService()