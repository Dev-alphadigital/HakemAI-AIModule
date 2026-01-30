"""
Azure Computer Vision OCR Service
==================================
Service for extracting text from images using Azure Computer Vision API.
Implements graceful degradation pattern - returns None on any failure.

Features:
- Singleton pattern for resource efficiency
- Rate limiting tracking (daily and per-minute)
- Circuit breaker pattern for API failures
- Comprehensive logging for cost tracking
- Async wrapper for synchronous Azure SDK

Author: HakemAI Team
Date: 2026-01-27
"""

import logging
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from io import BytesIO

from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials

from app.core.config import settings

logger = logging.getLogger(__name__)


class AzureOCRService:
    """
    Singleton service for Azure Computer Vision OCR.
    Extracts text from images with rate limiting and circuit breaker protection.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Azure client (initialized lazily)
        self.client: Optional[ComputerVisionClient] = None

        # Rate limiting counters
        self.daily_calls = 0
        self.daily_reset_time = datetime.now() + timedelta(days=1)
        self.minute_calls = 0
        self.minute_reset_time = datetime.now() + timedelta(minutes=1)

        # Circuit breaker
        self.consecutive_failures = 0
        self.circuit_open_until: Optional[datetime] = None
        self.max_failures = 3
        self.circuit_cooldown_seconds = 300  # 5 minutes

        self._initialized = True
        logger.info("🔧 AzureOCRService singleton initialized")

    def _initialize_client(self) -> bool:
        """
        Initialize Azure Computer Vision client.
        Returns True if successful, False otherwise.
        """
        try:
            if self.client is not None:
                return True

            # Check if credentials are configured
            if not settings.AZURE_VISION_ENDPOINT or not settings.AZURE_VISION_KEY:
                logger.warning("⚠️ Azure Computer Vision credentials not configured - OCR disabled")
                return False

            # Initialize client
            credentials = CognitiveServicesCredentials(settings.AZURE_VISION_KEY)
            self.client = ComputerVisionClient(settings.AZURE_VISION_ENDPOINT, credentials)

            logger.info(f"✅ Azure Computer Vision client initialized: {settings.AZURE_VISION_ENDPOINT}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize Azure Computer Vision client: {e}")
            return False

    def _check_rate_limits(self) -> bool:
        """
        Check if rate limits allow another API call.
        Resets counters if time windows have passed.

        Returns:
            True if call is allowed, False if rate limit exceeded
        """
        now = datetime.now()

        # Reset daily counter if needed
        if now >= self.daily_reset_time:
            self.daily_calls = 0
            self.daily_reset_time = now + timedelta(days=1)
            logger.info("🔄 Daily OCR call counter reset")

        # Reset minute counter if needed
        if now >= self.minute_reset_time:
            self.minute_calls = 0
            self.minute_reset_time = now + timedelta(minutes=1)

        # Check daily limit
        if self.daily_calls >= settings.OCR_MAX_DAILY_CALLS:
            logger.warning(f"⚠️ Daily OCR limit reached: {self.daily_calls}/{settings.OCR_MAX_DAILY_CALLS}")
            return False

        # Check per-minute limit (reasonable default: 20 calls/minute for standard tier)
        if self.minute_calls >= 20:
            logger.warning(f"⚠️ Per-minute OCR limit reached: {self.minute_calls}/20")
            return False

        return True

    def _check_circuit_breaker(self) -> bool:
        """
        Check if circuit breaker allows API calls.

        Returns:
            True if calls are allowed, False if circuit is open
        """
        if self.circuit_open_until is None:
            return True

        now = datetime.now()
        if now >= self.circuit_open_until:
            # Reset circuit breaker
            logger.info("🔄 Circuit breaker reset - retrying OCR calls")
            self.circuit_open_until = None
            self.consecutive_failures = 0
            return True

        time_remaining = (self.circuit_open_until - now).total_seconds()
        logger.warning(f"⚠️ Circuit breaker open - OCR calls paused for {time_remaining:.0f} more seconds")
        return False

    def _record_failure(self):
        """Record an API failure and potentially open circuit breaker."""
        self.consecutive_failures += 1

        if self.consecutive_failures >= self.max_failures:
            self.circuit_open_until = datetime.now() + timedelta(seconds=self.circuit_cooldown_seconds)
            logger.error(
                f"🚨 Circuit breaker opened after {self.consecutive_failures} consecutive failures - "
                f"pausing OCR for {self.circuit_cooldown_seconds} seconds"
            )

    def _record_success(self):
        """Record a successful API call."""
        self.consecutive_failures = 0
        self.daily_calls += 1
        self.minute_calls += 1

    def _parse_ocr_results(self, ocr_result) -> Optional[str]:
        """
        Extract text from Azure OCR response.

        Args:
            ocr_result: Azure Read API result object

        Returns:
            Concatenated text from all recognized lines, or None if no text found
        """
        try:
            if not ocr_result or not ocr_result.analyze_result:
                return None

            # Extract all text lines from all pages
            text_lines = []
            for read_result in ocr_result.analyze_result.read_results:
                for line in read_result.lines:
                    text_lines.append(line.text)

            if not text_lines:
                return None

            # Concatenate with spaces
            full_text = " ".join(text_lines)
            return full_text.strip() if full_text.strip() else None

        except Exception as e:
            logger.error(f"❌ Error parsing OCR results: {e}")
            return None

    async def analyze_image_for_text(self, image_bytes: bytes) -> Optional[str]:
        """
        Extract text from an image using Azure Computer Vision OCR.

        Args:
            image_bytes: Raw image bytes (JPEG, PNG, etc.)

        Returns:
            Extracted text as string, or None on any failure (graceful degradation)
        """
        start_time = datetime.now()

        try:
            # Check if OCR is enabled
            if not settings.ENABLE_OCR_FALLBACK:
                logger.debug("OCR fallback disabled in settings")
                return None

            # Initialize client if needed
            if not self._initialize_client():
                return None

            # Check circuit breaker
            if not self._check_circuit_breaker():
                return None

            # Check rate limits
            if not self._check_rate_limits():
                return None

            # Validate input
            if not image_bytes or len(image_bytes) == 0:
                logger.warning("⚠️ Empty image bytes provided to OCR")
                return None

            # Wrap synchronous Azure SDK call in thread
            logger.debug(f"📸 Starting OCR analysis for image ({len(image_bytes)} bytes)")

            image_stream = BytesIO(image_bytes)

            # Call Azure Read API (asynchronous operation)
            read_operation = await asyncio.to_thread(
                self.client.read_in_stream,
                image_stream,
                raw=True
            )

            # Get operation location (URL with operation ID)
            operation_location = read_operation.headers["Operation-Location"]
            operation_id = operation_location.split("/")[-1]

            # Wait for operation to complete (poll with timeout)
            max_attempts = 10
            attempt = 0

            while attempt < max_attempts:
                result = await asyncio.to_thread(
                    self.client.get_read_result,
                    operation_id
                )

                if result.status == OperationStatusCodes.succeeded:
                    # Parse and extract text
                    extracted_text = self._parse_ocr_results(result)

                    # Record success
                    self._record_success()

                    elapsed = (datetime.now() - start_time).total_seconds()
                    text_length = len(extracted_text) if extracted_text else 0

                    logger.info(
                        f"✅ OCR API call | {start_time.isoformat()} | success | "
                        f"text_length={text_length} | elapsed={elapsed:.2f}s | "
                        f"daily_calls={self.daily_calls}/{settings.OCR_MAX_DAILY_CALLS}"
                    )

                    return extracted_text

                elif result.status == OperationStatusCodes.failed:
                    logger.error("❌ Azure OCR operation failed")
                    self._record_failure()
                    return None

                # Still running, wait and retry
                attempt += 1
                await asyncio.sleep(1)

            # Timeout
            logger.error(f"❌ OCR operation timed out after {max_attempts} attempts")
            self._record_failure()
            return None

        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"❌ OCR API call | {start_time.isoformat()} | failure | "
                f"error={str(e)} | elapsed={elapsed:.2f}s"
            )
            self._record_failure()
            return None


# Global singleton instance
azure_ocr_service = AzureOCRService()
