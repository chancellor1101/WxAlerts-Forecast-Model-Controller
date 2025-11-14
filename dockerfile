FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# Install deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    gfortran \
    pkg-config \
    libaec-dev \
    libopenjp2-7-dev \
    libnetcdf-dev \
    netcdf-bin \
    libpng-dev \
    zlib1g-dev \
    libopenblas-dev \
    libomp-dev \
    libjpeg-dev \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip3 install --no-cache-dir \
    numpy \
    pandas \
    scipy \
    psycopg2-binary \
    redis \
    python-dotenv\
    rasterio \
    matplotlib \
    cartopy \
    pygrib \
    boto3

WORKDIR /app
COPY ingest_nbm.py /app/
COPY wgrib2 /app/
RUN chmod +x /app/ingest_nbm.py
RUN chmod +x /app/wgrib2

ENV PYTHONUNBUFFERED=1 TMP_DIR=/tmp/nbm
RUN mkdir -p /tmp/nbm

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pandas, scipy, psycopg2" 2>/dev/null && /usr/local/bin/wgrib2 -h > /dev/null 2>&1 || exit 1

CMD ["python3", "/app/ingest_nbm.py"]