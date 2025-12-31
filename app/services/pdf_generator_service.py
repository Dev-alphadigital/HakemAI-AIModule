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
        
        # Title style
        add_style_if_not_exists('CustomTitle', ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=HexColor('#2D5016'),  # Dark green
            spaceAfter=30,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        # Section heading
        add_style_if_not_exists('SectionHeading', ParagraphStyle(
            name='SectionHeading',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=HexColor('#4A7C2A'),  # Medium green
            spaceAfter=12,
            spaceBefore=20,
            fontName='Helvetica-Bold'
        ))
        
        # Subsection heading
        add_style_if_not_exists('SubsectionHeading', ParagraphStyle(
            name='SubsectionHeading',
            parent=self.styles['Heading3'],
            fontSize=14,
            textColor=HexColor('#6B9F3D'),  # Light green
            spaceAfter=8,
            spaceBefore=12,
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
                story.extend(self._build_strategic_memo(comparison_data))
                logger.info("✅ Strategic memo built successfully")
            except Exception as e:
                logger.error(f"❌ Error building strategic memo: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
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
            
            # Data Table section
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
            
            # Analytics/Charts section
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
        
        memo_info = [
            f"<b>TO:</b> Decision Makers | <b>FROM:</b> HAKEM.AI Platform | <b>DATE:</b> {datetime.now().strftime('%B %d, %Y')}",
            f"<b>SUBJECT:</b> Insurance Quote Comparison - Strategic Recommendation"
        ]
        
        for info in memo_info:
            story.append(Paragraph(info, memo_info_style))
        
        story.append(Spacer(1, 0.15*inch))
        
        # Executive Summary - Compact version
        summary = comparison_data.get("summary", {})
        key_differences = comparison_data.get("key_differences", {})
        
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
            
            # Strategic overview - more concise
            overview_text = f"""
            Analyzed <b>{total_providers} providers</b>. <b>{best_name}</b> ranks #1 with score {best_score:.1f} at SAR {best_premium:,.2f}. 
            Premium variance: {price_variance:.1f}% (SAR {price_range_low:,.2f} - SAR {price_range_high:,.2f}).
            """
            story.append(Paragraph(overview_text, self.styles['CustomBodyText']))
            story.append(Spacer(1, 0.1*inch))
        
        # Strategic Recommendation - more compact
        recommendation = key_differences.get("recommendation", "")
        recommendation_reasoning = key_differences.get("recommendation_reasoning", "")
        
        if recommendation:
            rec_text = f"""
            <b>Recommendation:</b> {recommendation}<br/>
            <b>Rationale:</b> {recommendation_reasoning if recommendation_reasoning else 'Best balance of coverage, price, and policy terms.'}
            """
            story.append(Paragraph(rec_text, self.styles['Highlight']))
            story.append(Spacer(1, 0.1*inch))
        
        # Top Providers Comparison - Compact table
        if len(ranking) >= 2:
            story.append(Paragraph("PROVIDER COMPARISON", self.styles['SubsectionHeading']))
            story.append(Spacer(1, 0.08*inch))
            
            alt_data = [["Rank", "Provider", "Score", "Premium (SAR)"]]
            
            for i, provider in enumerate(ranking[:4], 1):  # Top 4 providers
                if isinstance(provider, dict):
                    rank = str(i)
                    company = provider.get("company", "N/A")
                    score = f"{provider.get('score', 0):.1f}"
                    premium = f"{provider.get('premium', 0):,.2f}"
                    
                    # Create smaller font paragraph for company name
                    company_style = ParagraphStyle('CompactBody', parent=self.styles['CustomBodyText'], fontSize=8)
                    company_para = Paragraph(company, company_style)
                    alt_data.append([rank, company_para, score, premium])
            
            if len(alt_data) > 1:
                available_width = self.page_width - 1.5*inch
                alt_table = Table(alt_data, colWidths=[
                    available_width * 0.12,
                    available_width * 0.42,
                    available_width * 0.20,
                    available_width * 0.26
                ])
                alt_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(alt_table)
                story.append(Spacer(1, 0.1*inch))
        
        # Critical Decision Factors - Compact
        story.append(Paragraph("KEY DECISION FACTORS", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.08*inch))
        
        # Build decision factors from analytics
        analytics = comparison_data.get("analytics", {})
        statistics = analytics.get("statistics", {})
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        
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
        
        story.append(Spacer(1, 0.1*inch))
        
        return story
    
    def _build_detailed_comparison_factors(self, comparison_data: Dict[str, Any]) -> List:
        """
        ✨ DETAILED ANALYSIS: Granular technical report with recommendations.
        Comprehensive comparison of all factors with technical insights.
        """
        story = []
        
        # Clear section header for Detailed Analysis
        story.append(PageBreak())  # Ensure detailed analysis starts on new page
        detail_title = Paragraph("DETAILED TECHNICAL ANALYSIS", self.styles['CustomTitle'])
        story.append(detail_title)
        story.append(Spacer(1, 0.1*inch))
        
        # Subtitle explaining this is the detailed analysis section
        subtitle_style = ParagraphStyle(
            'DetailSubtitle',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.grey,
            alignment=TA_CENTER,
            spaceAfter=8
        )
        subtitle = Paragraph("Comprehensive Comparison with Technical Recommendations", subtitle_style)
        story.append(subtitle)
        story.append(Spacer(1, 0.15*inch))
        
        summary = comparison_data.get("summary", {})
        ranking = summary.get("ranking", [])
        side_by_side = comparison_data.get("side_by_side", {})
        key_differences = comparison_data.get("key_differences", {})
        data_table = comparison_data.get("data_table", {})
        rows = data_table.get("rows", [])
        
        # 1. Coverage Analysis
        story.append(Paragraph("1. Coverage Analysis", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.1*inch))
        
        if rows:
            # Coverage comparison table - use data_table rows for accurate data
            coverage_data = [["Provider", "Coverage Limit (SAR)", "Benefits Count", "Coverage Quality"]]
            
            for row in rows[:5]:  # Top 5 providers
                if isinstance(row, dict):
                    name = row.get("provider_name") or row.get("provider") or row.get("company") or "N/A"
                    
                    # Get coverage value
                    coverage_val = row.get("coverage") or row.get("coverage_limit") or 0
                    if isinstance(coverage_val, str):
                        # Extract number from string like "SR 1,564,652,306"
                        numbers = re.findall(r'[\d,]+', str(coverage_val).replace(" ", ""))
                        if numbers:
                            coverage = float(numbers[0].replace(",", ""))
                        else:
                            coverage = 0
                    else:
                        coverage = float(coverage_val) if coverage_val else 0
                    
                    # Get benefits count
                    benefits_val = row.get("benefits") or row.get("benefits_count") or 0
                    if isinstance(benefits_val, list):
                        benefits = len(benefits_val)
                    else:
                        benefits = int(benefits_val) if benefits_val else 0
                    
                    # Assess coverage quality
                    if benefits >= 15:
                        quality = "Comprehensive"
                    elif benefits >= 10:
                        quality = "Standard"
                    else:
                        quality = "Basic"
                    
                    name_para = Paragraph(str(name), self.styles['CustomBodyText'])
                    coverage_data.append([
                        name_para,
                        f"{coverage:,.0f}" if coverage > 0 else "N/A",
                        str(benefits) if benefits > 0 else "0",
                        quality
                    ])
            
            if len(coverage_data) > 1:
                available_width = self.page_width - 1.5*inch
                coverage_table = Table(coverage_data, colWidths=[
                    available_width * 0.35,
                    available_width * 0.25,
                    available_width * 0.2,
                    available_width * 0.2
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
                    ('TOPPADDING', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(coverage_table)
                story.append(Spacer(1, 0.1*inch))
                
                # Technical Recommendation for Coverage
                tech_rec_style = ParagraphStyle('TechRec', parent=self.styles['CustomBodyText'], 
                                               fontSize=9, textColor=HexColor('#2D5016'), 
                                               leftIndent=0.2*inch, spaceAfter=6)
                story.append(Paragraph("<b>✓ Technical Recommendation:</b> Select providers with 'Comprehensive' coverage quality. "
                                     "Verify coverage limits align with asset values. Request schedule of properties if needed.",
                                     tech_rec_style))
                story.append(Spacer(1, 0.15*inch))
        
        # 2. Risk Assessment
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
        
        # 3. Value Optimization
        story.append(Paragraph("3. Value Optimization Analysis", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.1*inch))
        
        if rows:
            # Calculate value scores (benefits per SAR spent) - use data_table rows
            value_data = [["Provider", "Premium (SAR)", "Benefits Count", "Value Score"]]
            
            for row in rows[:5]:
                if isinstance(row, dict):
                    name = row.get("provider_name") or row.get("provider") or row.get("company") or "N/A"
                    
                    # Get premium value
                    premium_val = row.get("premium") or row.get("premium_amount") or 0
                    premium = float(premium_val) if premium_val else 0
                    
                    # Get benefits count
                    benefits_val = row.get("benefits") or row.get("benefits_count") or 0
                    if isinstance(benefits_val, list):
                        benefits = len(benefits_val)
                    else:
                        benefits = int(benefits_val) if benefits_val else 0
                    
                    # Calculate value score (benefits per 1000 SAR)
                    value_score = (benefits / premium * 1000) if premium > 0 and benefits > 0 else 0
                    
                    name_para = Paragraph(str(name), self.styles['CustomBodyText'])
                    value_data.append([
                        name_para,
                        f"{premium:,.2f}" if premium > 0 else "N/A",
                        str(benefits) if benefits > 0 else "0",
                        f"{value_score:.2f}" if value_score > 0 else "0.00"
                    ])
            
            if len(value_data) > 1:
                available_width = self.page_width - 1.5*inch
                value_table = Table(value_data, colWidths=[
                    available_width * 0.35,
                    available_width * 0.25,
                    available_width * 0.2,
                    available_width * 0.2
                ])
                value_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#4A7C2A')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('TOPPADDING', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]),
                ]))
                story.append(value_table)
                
                story.append(Spacer(1, 0.1*inch))
                value_note = Paragraph(
                    "<i>Value Score = Benefits per 1,000 SAR premium. Higher scores indicate better cost-efficiency.</i>",
                    self.styles['CustomBodyText']
                )
                story.append(value_note)
                story.append(Spacer(1, 0.08*inch))
                
                # Technical Recommendation for Value Optimization
                tech_rec_style = ParagraphStyle('TechRec', parent=self.styles['CustomBodyText'], 
                                               fontSize=9, textColor=HexColor('#2D5016'), 
                                               leftIndent=0.2*inch, spaceAfter=6)
                story.append(Paragraph("<b>✓ Technical Recommendation:</b> Balance value score with coverage quality. "
                                     "Lowest premium doesn't always mean best value. Negotiate multi-year contracts for premium stability. "
                                     "Request claims history data from top 3 providers.",
                                     tech_rec_style))
                story.append(Spacer(1, 0.15*inch))
        
        # 3.5. Benefits per Provider
        story.append(Paragraph("3.5. Benefits Comparison per Provider", self.styles['SubsectionHeading']))
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
        
        # 4. Policy Terms & Conditions
        story.append(Paragraph("4. Policy Terms & Subjectivities", self.styles['SubsectionHeading']))
        story.append(Spacer(1, 0.1*inch))
        
        terms_intro = """
        Policy subjectivities are conditions that must be met for coverage to apply. 
        Understanding these terms is essential for ensuring claims are processed successfully.
        """
        story.append(Paragraph(terms_intro, self.styles['CustomBodyText']))
        story.append(Spacer(1, 0.1*inch))
        
        if providers_data:
            for provider in providers_data[:3]:
                provider_name = provider.get("name", "Unknown")
                subjectivities = provider.get("subjectivities", [])
                
                if subjectivities:
                    story.append(Paragraph(f"<b>{provider_name}</b>", self.styles['CompanyName']))
                    
                    subj_count = 0
                    for subj in subjectivities[:5]:  # Top 5 subjectivities
                        subj_text = str(subj) if isinstance(subj, str) else subj.get("text", str(subj))
                        if self._is_valid_item_text(subj_text):
                            story.append(Paragraph(f"• {subj_text}", self.styles['CustomBodyText']))
                            subj_count += 1
                    
                    if subj_count == 0:
                        story.append(Paragraph("• Standard policy terms apply", self.styles['CustomBodyText']))
                    
                    story.append(Spacer(1, 0.1*inch))
        
        # Technical Recommendation for Policy Terms
        tech_rec_style = ParagraphStyle('TechRec', parent=self.styles['CustomBodyText'], 
                                       fontSize=9, textColor=HexColor('#2D5016'), 
                                       leftIndent=0.2*inch, spaceAfter=6)
        story.append(Paragraph("<b>✓ Technical Recommendation:</b> Document all subjectivities and assign compliance ownership. "
                             "Schedule risk surveys within required timeframes. Establish monitoring systems for ongoing warranty compliance. "
                             "Non-compliance may void coverage - create compliance checklist before binding.",
                             tech_rec_style))
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
        """Build analytics/charts section with charts and statistics (NO HAKIM SCORE)."""
        story = []
        
        title = Paragraph("Charts", self.styles['SectionHeading'])
        story.append(title)
        story.append(Spacer(1, 0.15*inch))
        
        analytics = comparison_data.get("analytics", {})
        summary = comparison_data.get("summary", {})
        ranking = summary.get("ranking", []) if summary else []
        
        # Statistics (NO HAKIM SCORE)
        statistics = analytics.get("statistics", {})
        if statistics:
            story.append(Paragraph("Summary Statistics", self.styles['SubsectionHeading']))
            
            stats_data = []
            if statistics.get("average_score"):
                stats_data.append(["Average Score:", f"{statistics.get('average_score', 0):.1f}%"])  # Direct % symbol
            if statistics.get("best_score"):
                stats_data.append(["Best Score:", f"{statistics.get('best_score', 0):.1f}%"])  # Direct % symbol
            if statistics.get("average_premium"):
                stats_data.append(["Average Premium (SAR):", f"{statistics.get('average_premium', 0):,.2f}"])
            if statistics.get("lowest_premium"):
                stats_data.append(["Lowest Premium (SAR):", f"{statistics.get('lowest_premium', 0):,.2f}"])
            if statistics.get("highest_premium"):
                stats_data.append(["Highest Premium (SAR):", f"{statistics.get('highest_premium', 0):,.2f}"])
            
            if stats_data:
                available_width = self.page_width - 1.8*inch
                stats_table = Table(stats_data, colWidths=[available_width * 0.4, available_width * 0.6])
                stats_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, -1), HexColor('#E8F5E9')),
                    ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                ]))
                story.append(stats_table)
                story.append(Spacer(1, 0.2*inch))
        
        # Generate Premium Comparison Chart (CRITICAL FIX: Sort by premium, not rank!)
        if ranking:
            story.append(Paragraph("Premium Comparison (Lowest to Highest)", self.styles['SubsectionHeading']))
            story.append(Spacer(1, 0.1*inch))
            
            # CRITICAL FIX: Sort by premium amount, NOT by overall rank
            # This is a "Premium Comparison", so it must be sorted by premium!
            sorted_by_premium = sorted(ranking, key=lambda x: x.get("premium", float('inf')))[:10]
            
            # Create premium comparison table with premium-based ranking
            table_data = [["Provider", "Premium (SAR)", "Premium Rank"]]
            for premium_rank, item in enumerate(sorted_by_premium, 1):
                if isinstance(item, dict):
                    provider = item.get("company", "N/A")
                    premium = item.get("premium", 0)
                    # CRITICAL FIX: Wrap provider name in Paragraph
                    provider_paragraph = Paragraph(provider, self.styles['CustomBodyText'])
                    # Premium rank is based on actual premium order (1 = lowest)
                    table_data.append([provider_paragraph, f"{premium:,.2f}", str(premium_rank)])
            
            if len(table_data) > 1:
                available_width = self.page_width - 1.8*inch
                chart_table = Table(table_data, colWidths=[available_width * 0.4, available_width * 0.4, available_width * 0.2])
                chart_style = [
                    ('BACKGROUND', (0, 0), (-1, 0), HexColor('#6B9F3D')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Provider column left-aligned
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),  # Other columns center
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('WORDWRAP', (0, 0), (-1, -1), True),  # CRITICAL FIX: Enable word wrap
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                    ('TOPPADDING', (0, 0), (-1, 0), 6),
                ]
                if len(table_data) > 2:
                    chart_style.append(('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F5F5F5')]))
                chart_table.setStyle(TableStyle(chart_style))
                story.append(chart_table)
                story.append(Spacer(1, 0.2*inch))
        
        # Overall Score Comparison (NO HAKIM SCORE)
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
        
        # Key Insights
        insights = analytics.get("insights", [])
        if insights:
            # Filter out "Hakim Score integration" or metadata insights
            filtered_insights = [
                insight for insight in insights
                if not any(keyword in str(insight).lower() for keyword in [
                    'hakim score integration',
                    'integration: enabled',
                    'integration enabled'
                ])
            ]
            
            if filtered_insights:
                story.append(Paragraph("Key Insights", self.styles['SubsectionHeading']))
                story.append(Spacer(1, 0.1*inch))
                
                for insight in filtered_insights[:5]:  # Limit to 5 insights
                    insight_text = Paragraph(f"• {insight}", self.styles['CustomBodyText'])
                    story.append(insight_text)
                    story.append(Spacer(1, 0.08*inch))
        
        return story


# Global singleton instance
pdf_generator_service = PDFGeneratorService()