import time
import asyncio
import logging
from typing import Dict, Optional, Any
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    Singleton progress tracker for managing job progress state.
    
    Thread-safe in-memory storage with automatic cleanup.
    Optimized for hundreds of concurrent users.
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ProgressTracker, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._store: Dict[str, Dict[str, Any]] = {}
        self._result_store: Dict[str, Dict[str, Any]] = {}  # Store final results temporarily
        self._store_lock = Lock()
        self._cleanup_task = None
        self._initialized = True
        
        logger.info("✅ Progress Tracker initialized")
    
    def initialize_progress(
        self, 
        job_id: str, 
        total_files: int,
        comparison_id: Optional[str] = None
    ) -> None:
        """
        Initialize progress tracking for a new job.
        
        Args:
            job_id: Unique job identifier
            total_files: Total number of files to process
            comparison_id: Optional comparison ID (if already generated)
        """
        with self._store_lock:
            current_time = time.time()
            self._store[job_id] = {
                "job_id": job_id,
                "comparison_id": comparison_id,
                "status": "processing",
                "percentage": 0,
                "current_step": "Initializing",
                "step_details": f"Preparing to process {total_files} file(s)",
                "total_files": total_files,
                "files_processed": 0,
                "current_file_index": 0,
                "sub_step": None,
                "created_at": current_time,
                "last_update": current_time,
                "estimated_seconds_remaining": None,
                "error_message": None
            }
        
        logger.info(f"📊 Progress initialized for job: {job_id} ({total_files} files)")
    
    def update_progress(
        self,
        job_id: str,
        step_name: str,
        percentage: float,
        details: Optional[str] = None,
        sub_step: Optional[str] = None,
        files_processed: Optional[int] = None,
        current_file_index: Optional[int] = None,
        estimated_seconds: Optional[int] = None
    ) -> None:
        """
        Update progress for a job.
        
        Args:
            job_id: Job identifier
            step_name: Name of current step
            percentage: Progress percentage (0-100)
            details: Additional step details
            sub_step: Current sub-step (e.g., "Extracting text")
            files_processed: Number of files completed
            current_file_index: Current file being processed (1-indexed)
            estimated_seconds: Estimated seconds remaining
        """
        with self._store_lock:
            if job_id not in self._store:
                logger.warning(f"⚠️  Attempted to update non-existent job: {job_id}")
                return
            
            progress = self._store[job_id]
            progress["percentage"] = min(100, max(0, percentage))
            progress["current_step"] = step_name
            progress["last_update"] = time.time()
            
            if details is not None:
                progress["step_details"] = details
            if sub_step is not None:
                progress["sub_step"] = sub_step
            if files_processed is not None:
                progress["files_processed"] = files_processed
            if current_file_index is not None:
                progress["current_file_index"] = current_file_index
            if estimated_seconds is not None:
                progress["estimated_seconds_remaining"] = estimated_seconds
    
    def mark_completed(
        self,
        job_id: str,
        comparison_id: str,
        result: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Mark a job as completed and store result.
        
        Args:
            job_id: Job identifier
            comparison_id: Comparison ID for the result
            result: Final result data (optional, can be fetched separately)
        """
        with self._store_lock:
            if job_id not in self._store:
                logger.warning(f"⚠️  Attempted to complete non-existent job: {job_id}")
                return
            
            progress = self._store[job_id]
            progress["status"] = "completed"
            progress["percentage"] = 100
            progress["current_step"] = "Complete"
            progress["step_details"] = "Processing completed successfully"
            progress["comparison_id"] = comparison_id
            progress["last_update"] = time.time()
            progress["estimated_seconds_remaining"] = 0
            
            # Store result temporarily (for 5 minutes)
            if result is not None:
                self._result_store[job_id] = {
                    "job_id": job_id,
                    "comparison_id": comparison_id,
                    "result": result,
                    "completed_at": time.time()
                }
        
        logger.info(f"✅ Job completed: {job_id} → comparison_id: {comparison_id}")
    
    def mark_error(
        self,
        job_id: str,
        error_message: str
    ) -> None:
        """
        Mark a job as failed.
        
        Args:
            job_id: Job identifier
            error_message: Error message
        """
        with self._store_lock:
            if job_id not in self._store:
                return
            
            progress = self._store[job_id]
            progress["status"] = "error"
            progress["current_step"] = "Error"
            progress["step_details"] = f"Processing failed: {error_message}"
            progress["error_message"] = error_message
            progress["last_update"] = time.time()
        
        logger.error(f"❌ Job failed: {job_id} - {error_message}")
    
    def get_progress(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current progress for a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Progress data dict or None if not found
        """
        with self._store_lock:
            if job_id not in self._store:
                return None
            
            progress = self._store[job_id].copy()
            
            # Add human-readable timestamp
            progress["timestamp"] = datetime.fromtimestamp(
                progress["last_update"]
            ).isoformat()
            
            return progress
    
    def get_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get stored result for a completed job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Result data or None if not found
        """
        with self._store_lock:
            return self._result_store.get(job_id)
    
    def cleanup(self, job_id: str) -> None:
        """
        Remove progress and result data for a job.
        
        Args:
            job_id: Job identifier
        """
        with self._store_lock:
            self._store.pop(job_id, None)
            self._result_store.pop(job_id, None)
        
        logger.debug(f"🧹 Cleaned up job: {job_id}")
    
    def cleanup_old_entries(self, max_age_seconds: int = 3600) -> int:
        """
        Remove old/stale progress entries.
        
        Args:
            max_age_seconds: Maximum age in seconds before cleanup (default: 1 hour)
            
        Returns:
            Number of entries cleaned up
        """
        current_time = time.time()
        expired_jobs = []
        
        with self._store_lock:
            for job_id, data in self._store.items():
                age = current_time - data["last_update"]
                
                # Remove if:
                # - Older than max_age_seconds, OR
                # - Completed for more than 5 minutes, OR
                # - Error status for more than 30 minutes
                should_remove = False
                
                if age > max_age_seconds:
                    should_remove = True
                elif data["status"] == "completed" and age > 300:  # 5 minutes
                    should_remove = True
                elif data["status"] == "error" and age > 1800:  # 30 minutes
                    should_remove = True
                
                if should_remove:
                    expired_jobs.append(job_id)
            
            # Remove expired jobs
            for job_id in expired_jobs:
                self._store.pop(job_id, None)
                self._result_store.pop(job_id, None)
        
        if expired_jobs:
            logger.info(f"🧹 Cleaned up {len(expired_jobs)} old progress entries")
        
        return len(expired_jobs)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about current progress tracking.
        
        Returns:
            Dictionary with statistics
        """
        with self._store_lock:
            total = len(self._store)
            status_counts = {
                "processing": 0,
                "completed": 0,
                "error": 0
            }
            
            for data in self._store.values():
                status = data.get("status", "unknown")
                if status in status_counts:
                    status_counts[status] += 1
            
            return {
                "total_jobs": total,
                "status_breakdown": status_counts,
                "stored_results": len(self._result_store)
            }


# Global singleton instance
progress_tracker = ProgressTracker()


async def start_cleanup_task(interval_seconds: int = 300):
    """
    Start background cleanup task.
    
    Args:
        interval_seconds: How often to run cleanup (default: 5 minutes)
    """
    logger.info(f"🔄 Starting progress cleanup task (interval: {interval_seconds}s)")
    
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            cleaned = progress_tracker.cleanup_old_entries()
            if cleaned > 0:
                logger.info(f"🧹 Cleanup task: Removed {cleaned} old entries")
        except Exception as e:
            logger.error(f"❌ Cleanup task error: {e}")


