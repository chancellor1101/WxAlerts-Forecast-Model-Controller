# Dockerfile for NBM Weather Ingestion
# Use Ubuntu base for easier wgrib2 installation
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies including wgrib2
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wgrib2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    numpy \
    pandas \
    scipy \
    psycopg2-binary \
    redis \
    python-dotenv

# Verify wgrib2 is available
RUN which wgrib2 && (wgrib2 -version || true)

# Create app directory
WORKDIR /app

# Copy Python scripts
COPY ingest_nbm.py /app/
# Or use the smart version:
# COPY ingest_nbm_smart.py /app/ingest_nbm.py

# Make scripts executable
RUN chmod +x /app/*.py

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV TMP_DIR=/tmp/nbm

# Create temp directory
RUN mkdir -p /tmp/nbm

# Health check - verify Python packages
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pandas; import scipy; import psycopg2; print('OK')" || exit 1

# Run the ingestion script
CMD ["python3", "/app/ingest_nbm.py"]