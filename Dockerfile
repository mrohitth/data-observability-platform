# Data Observability Platform - Dockerfile
# Multi-stage build for production deployment

# Build stage
FROM python:3.11-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Production stage
FROM python:3.11-slim as production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* && \
    apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create application user
RUN groupadd -r observability && \
    useradd -r -g observability -d /app -s /bin/bash observability

# Create application directory
WORKDIR /app
RUN mkdir -p logs config && \
    chown -R observability:observability /app

# Copy application code
COPY --chown=observability:observability src/ ./src/
COPY --chown=observability:observability scripts/ ./scripts/
COPY --chown=observability:observability config/ ./config/
COPY --chown=observability:observability .env.example .env.example

# Switch to non-root user
USER observability

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command
CMD ["python", "src/production_observability_engine.py"]

# Labels
LABEL maintainer="Data Observability Team <team@dataobservability.com>" \
      version="1.0.0" \
      description="Data Observability Platform - Comprehensive data monitoring and reliability"
