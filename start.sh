#!/bin/bash

# ============================================================================
# Production Startup Script for DigitalOcean
# ============================================================================
# This script starts the FastAPI application using Gunicorn with Uvicorn workers
# Optimized for production deployment on DigitalOcean

set -e  # Exit on error

echo "=========================================="
echo "🚀 Starting HakemAI API Server"
echo "=========================================="

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "📝 Loading environment variables from .env"
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "⚠️  No .env file found, using system environment variables"
fi

# Set default values if not provided
export PORT=${PORT:-8000}
export WORKERS=${GUNICORN_WORKERS:-4}
export LOG_LEVEL=${GUNICORN_LOG_LEVEL:-info}

echo "Configuration:"
echo "  - Port: $PORT"
echo "  - Workers: $WORKERS"
echo "  - Log Level: $LOG_LEVEL"
echo "=========================================="

# Create necessary directories
mkdir -p uploads logs

# Start Gunicorn with Uvicorn workers
echo "🎯 Starting Gunicorn with Uvicorn workers..."
exec gunicorn main:app \
    --config gunicorn.conf.py \
    --log-file - \
    --access-logfile - \
    --error-logfile -

