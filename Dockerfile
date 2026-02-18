# ──────────────────────────────────────────────────────────────
# Weather ETL Pipeline – Production Docker Image
# ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base

LABEL maintainer="data-engineering-team"
LABEL description="Weather ETL pipeline – Open-Meteo API"

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create runtime directories
RUN mkdir -p data logs

# Default: one-shot run.  Override CMD to add --schedule for recurring mode.
CMD ["python", "main.py"]
