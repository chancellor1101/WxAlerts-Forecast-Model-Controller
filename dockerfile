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
    numpy pandas scipy psycopg2-binary redis python-dotenv

# Build ip
RUN git clone -b develop https://github.com/NOAA-EMC/NCEPLIBS-ip.git /tmp/ip && \
    mkdir -p /tmp/ip/build && cd /tmp/ip/build && \
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local .. && \
    make -j$(nproc) && make install && \
    rm -rf /tmp/ip

# Build g2c
RUN git clone -b develop https://github.com/NOAA-EMC/NCEPLIBS-g2c.git /tmp/g2c && \
    mkdir -p /tmp/g2c/build && cd /tmp/g2c/build && \
    cmake .. -DUSE_Jasper=OFF -DUSE_OpenJPEG=ON && \
    make -j$(nproc) && make install && \
    rm -rf /tmp/g2c

# Build wgrib2
RUN git clone -b develop https://github.com/NOAA-EMC/wgrib2.git /tmp/wgrib2 && \
    mkdir -p /tmp/wgrib2/build && cd /tmp/wgrib2/build && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DUSE_NETCDF=ON \
        -DUSE_OpenJPEG=ON \
        -DUSE_PNG=ON \
        -DUSE_AEC=ON \
        -DUSE_IPOLATES=ON && \
    make -j$(nproc) && \
    cp wgrib2/wgrib2 /usr/local/bin/ && \
    rm -rf /tmp/wgrib2

# App
WORKDIR /app
COPY ingest_nbm.py /app/
RUN chmod +x /app/ingest_nbm.py

ENV PYTHONUNBUFFERED=1 TMP_DIR=/tmp/nbm
RUN mkdir -p /tmp/nbm

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pandas, scipy, psycopg2" 2>/dev/null && /usr/local/bin/wgrib2 -h > /dev/null 2>&1 || exit 1

CMD ["python3", "/app/ingest_nbm.py"]