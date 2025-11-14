#!/usr/bin/env python3
import os
import time
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import rasterio
from rasterio.transform import from_bounds
import pygrib
import redis
import boto3

class NBMToGeoTIFFConverter:
    def __init__(self, output_dir='./nbm_output'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)

        self.tiffs_dir = self.output_dir / 'tiffs'
        self.tiffs_dir.mkdir(exist_ok=True)

        # ENV VARS
        self.max_fhr = int(os.getenv("MAX_FHR", 36))
        self.redis = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
        
        # S3-compatible storage configuration
        self.bucket = os.getenv("S3_BUCKET")
        if not self.bucket:
            raise ValueError("S3_BUCKET environment variable is not set")
        
        s3_config = {
            "region_name": os.getenv("AWS_REGION", "us-east-1")
        }
        
        # Add endpoint URL for MinIO/R2
        endpoint_url = os.getenv("S3_ENDPOINT_URL")
        if endpoint_url:
            s3_config["endpoint_url"] = endpoint_url
        
        self.s3 = boto3.client("s3", **s3_config)                                 

        self.variables = {
            'temp': {'name': '2 metre temperature', 'transform': lambda x: (x - 273.15) * 1.8 + 32},
            'rh': {'name': '2 metre relative humidity', 'transform': lambda x: x},
            'wind': {'name': '10 metre wind speed', 'transform': lambda x: x},
            'wdir': {'name': '10 metre wind direction', 'transform': lambda x: x},
            'gust': {'name': 'Instantaneous 10 metre wind gust', 'transform': lambda x: x * 2.23694},
            'vis': {'name': 'Visibility', 'transform': lambda x: x * 0.000621371},
            'cape': {'name': 'Convective available potential energy', 'transform': lambda x: x},
        }

    def log(self, msg):
        print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)

    def upload_to_s3(self, local_path: Path):
        key = f"nbm/{local_path.name}"
        self.s3.upload_file(str(local_path), self.bucket, key,
                ExtraArgs={
            'Metadata': {
                'cycle_date': datetime.utcnow().date,
                'cycle_hour': datetime.utcnow().timestamp,
                'generated': datetime.utcnow().isoformat()
            }
        }
        );
        self.log(f"✓ Uploaded to S3: {key}")

    def get_last_processed_cycle(self):
        d = self.redis.get("nbm:last_cycle_date")
        h = self.redis.get("nbm:last_cycle_hour")
        return (d.decode(), h.decode()) if d and h else (None, None)

    def set_last_processed_cycle(self, date, hour):
        self.redis.set("nbm:last_cycle_date", date)
        self.redis.set("nbm:last_cycle_hour", hour)

    def get_latest_nbm_cycle(self):
        now = datetime.utcnow()
        cycles = [0, 6, 12, 18]
        valid = [c for c in cycles if now.hour >= c + 3]

        if valid:
            return now.strftime("%Y%m%d"), f"{max(valid):02d}"
        else:
            prev = now - timedelta(days=1)
            return prev.strftime("%Y%m%d"), "18"

    def download_grib(self, run_date, run_hour, fhr):
        url = f"https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{run_date}/{run_hour}/core/blend.t{run_hour}z.core.f{fhr:03d}.co.grib2"
        out = self.tiffs_dir.parent / f"f{fhr:03d}.grib2"
        if out.exists():
            return out

        import urllib.request
        self.log(f"Downloading f{fhr:03d}...")
        data = urllib.request.urlopen(url, timeout=120).read()
        with open(out, "wb") as f:
            f.write(data)
        return out

    def process_fhr(self, run_date, run_hour, fhr):
        grib_path = self.download_grib(run_date, run_hour, fhr)
        grbs = pygrib.open(grib_path)

        band_data = {}
        lats = lons = None

        for key, cfg in self.variables.items():
            try:
                msg = grbs.select(name=cfg["name"])[0]
                arr, lat, lon = msg.data()
                arr = cfg["transform"](arr.astype(np.float32))
                band_data[key] = arr
                if lats is None:
                    lats, lons = lat, lon
            except:
                continue

        if not band_data:
            return

        h, w = lats.shape
        tiff_path = self.tiffs_dir / f"f{fhr:03d}.tif"
        transform = from_bounds(lons.min(), lats.min(), lons.max(), lats.max(), w, h)

        with rasterio.open(
            tiff_path,
            "w",
            driver="GTiff",
            height=h,
            width=w,
            count=len(band_data),
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
            compress="DEFLATE",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        ) as dst:
            for i, (key, arr) in enumerate(band_data.items(), 1):
                dst.write(arr, i)
                dst.set_band_description(i, key)

        self.upload_to_s3(tiff_path)

    def run_forever(self):
        while True:
            run_date, run_hour = self.get_latest_nbm_cycle()
            last_date, last_hour = self.get_last_processed_cycle()

            if (run_date == last_date and run_hour == last_hour):
                self.log("No new NBM cycle yet. Sleeping 20m.")
                time.sleep(1200)
                continue

            self.log(f"Processing new cycle: {run_date} {run_hour}Z")
            for fhr in range(1, self.max_fhr + 1):
                self.process_fhr(run_date, run_hour, fhr)

            self.set_last_processed_cycle(run_date, run_hour)
            self.log(f"Cycle {run_date} {run_hour}Z complete.")
            time.sleep(300)

if __name__ == "__main__":
    NBMToGeoTIFFConverter().run_forever()
