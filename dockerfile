# Dockerfile for NBM Weather Ingestion
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    gfortran \
    curl \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    numpy \
    pandas \
    scipy \
    psycopg2-binary \
    redis \
    python-dotenv

# Install wgrib2 (minimal build, no NetCDF needed)
WORKDIR /tmp
RUN wget https://www.ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz && \
    tar -xzf wgrib2.tgz && \
    cd grib2 && \
    export CC=gcc && \
    export FC=gfortran && \
    make DISABLE_NETCDF=1 DISABLE_JASPER=1 DISABLE_PNG=1 DISABLE_IPOLATES=1 && \
    cp wgrib2/wgrib2 /usr/local/bin/ && \
    cd / && \
    rm -rf /tmp/grib2 /tmp/wgrib2.tgz

# Verify wgrib2 installation (wgrib2 -version returns exit code 8, so we just check if it exists)
RUN which wgrib2 && wgrib2 -version || true

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

# Health check - verify wgrib2 and Python packages
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pandas; import scipy; import psycopg2; print('OK')" && \
        which wgrib2 || exit 1

# Run the ingestion script
CMD ["python3", "/app/ingest_nbm.py"]