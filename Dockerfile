# Enterprise Healthcare Claims Lakehouse Platform Dockerfile
# Multi-stage build for production deployment

# Base stage - Python environment
FROM python:3.9-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    unzip \
    openjdk-11-jdk \
    gnupg \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash healthcare

# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Production stage
FROM base as production

# Copy application code
COPY . .

# Set ownership to healthcare user
RUN chown -R healthcare:healthcare /app

# Switch to healthcare user
USER healthcare

# Expose ports for potential web interfaces
EXPOSE 8080 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command
CMD ["python", "-m", "src.main"]

# Development stage
FROM base as development

# Install development dependencies
RUN pip install jupyterlab pytest black flake8 mypy

# Copy development code
COPY . .

# Set ownership
RUN chown -R healthcare:healthcare /app

# Switch to healthcare user
USER healthcare

# Development command
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

# Databricks runtime stage
FROM mcr.microsoft.com/databricks-runtime:10.4 as databricks

# Copy notebooks and source code
COPY notebooks/ /databricks/notebooks/
COPY src/ /databricks/src/
COPY data/ /databricks/data/

# Set permissions
RUN chmod +x /databricks/notebooks/*.py

# Default entrypoint for Databricks
ENTRYPOINT ["/databricks/entrypoint.sh"]

# Monitoring stage
FROM prom/prometheus:latest as monitoring

# Copy Prometheus configuration
COPY monitoring/prometheus.yml /etc/prometheus/

# Expose metrics port
EXPOSE 9090

# Grafana stage
FROM grafana/grafana:latest as grafana

# Copy Grafana dashboards and configuration
COPY monitoring/grafana/ /etc/grafana/provisioning/

# Expose Grafana port
EXPOSE 3000

# Build arguments for customization
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

# Labels for metadata
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="Enterprise Healthcare Claims Lakehouse" \
      org.label-schema.description="Azure-based healthcare data platform with batch and streaming processing" \
      org.label-schema.url="https://github.com/your-org/healthcare-lakehouse" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/your-org/healthcare-lakehouse.git" \
      org.label-schema.vendor="Healthcare Analytics Team" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"
