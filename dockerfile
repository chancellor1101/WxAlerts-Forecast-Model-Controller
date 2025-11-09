# Dockerfile for NBM Weather Ingestion
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    gfortran \
    libnetcdf-dev \
    libhdf5-dev \
    libpng-dev \
    curl \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    netCDF4 \
    numpy \
    psycopg2-binary \
    redis \
    python-dotenv \
    scipy \
    pandas \
    redis

# Install wgrib2 with NetCDF support
WORKDIR /tmp
RUN wget https://www.ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/wgrib2.tgz && \
    tar -xzf wgrib2.tgz && \
    cd grib2 && \
    export CC=gcc && \
    export FC=gfortran && \
    # Enable NetCDF in makefile
    sed -i 's/#USE_NETCDF4=1/USE_NETCDF4=1/' makefile && \
    sed -i 's/#USE_NETCDF3=1/USE_NETCDF3=1/' makefile && \
    make && \
    cp wgrib2/wgrib2 /usr/local/bin/ && \
    cd / && \
    rm -rf /tmp/grib2 /tmp/wgrib2.tgz

# Create app directory
WORKDIR /app

# Copy Python scripts
COPY extract_nbm_netcdf.py /app/
COPY ingest_nbm.py /app/

# Make scripts executable
RUN chmod +x /app/*.py

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV TMP_DIR=/tmp/nbm

# Create temp directory
RUN mkdir -p /tmp/nbm

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import netCDF4; print('OK')" || exit 1

# Run the ingestion script
CMD ["python3", "/app/ingest_nbm.py"]