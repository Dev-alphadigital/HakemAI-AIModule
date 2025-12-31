"""
File handling utilities for PDF uploads and management.
"""

import os
import shutil
from pathlib import Path
from typing import List
from fastapi import UploadFile, HTTPException
from app.core.config import settings


async def save_uploaded_file(file: UploadFile) -> Path:
    """
    Save an uploaded PDF file to the uploads directory.
    
    Args:
        file: FastAPI UploadFile object
        
    Returns:
        Path to the saved file
        
    Raises:
        HTTPException: If file validation fails
    """
    # Validate file extension
    file_extension = Path(file.filename).suffix.lower()
    if file_extension not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Allowed: {', '.join(settings.ALLOWED_EXTENSIONS)}"
        )
    
    # Create a safe filename (prevent path traversal attacks)
    safe_filename = Path(file.filename).name
    file_path = Path(settings.UPLOAD_DIR) / safe_filename
    
    # Handle duplicate filenames by appending a number
    counter = 1
    while file_path.exists():
        stem = Path(safe_filename).stem
        file_path = Path(settings.UPLOAD_DIR) / f"{stem}_{counter}{file_extension}"
        counter += 1
    
    try:
        # Save file in chunks to handle large files efficiently
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Check file size
        file_size = file_path.stat().st_size
        if file_size > settings.MAX_FILE_SIZE:
            file_path.unlink()  # Delete the file
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Maximum size: {settings.MAX_FILE_SIZE / (1024*1024)}MB"
            )
        
        return file_path
    
    except Exception as e:
        # Clean up on error
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")


async def save_multiple_files(files: List[UploadFile]) -> List[Path]:
    """
    Save multiple uploaded PDF files.
    
    Args:
        files: List of FastAPI UploadFile objects
        
    Returns:
        List of paths to saved files
    """
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="No files provided")
    
    saved_paths = []
    try:
        for file in files:
            path = await save_uploaded_file(file)
            saved_paths.append(path)
        return saved_paths
    
    except Exception as e:
        # Clean up all saved files on error
        cleanup_files(saved_paths)
        raise e


def cleanup_files(file_paths: List[Path]) -> None:
    """
    Delete temporary files after processing.
    
    Args:
        file_paths: List of file paths to delete
    """
    for path in file_paths:
        try:
            if path.exists():
                path.unlink()
        except Exception as e:
            print(f"Warning: Failed to delete {path}: {str(e)}")


def get_file_size_mb(file_path: Path) -> float:
    """Get file size in megabytes."""
    return file_path.stat().st_size / (1024 * 1024)