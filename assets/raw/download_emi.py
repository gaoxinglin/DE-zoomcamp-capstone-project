""" @bruin

name: raw.download_emi
type: python
description: "Download EMI monthly CSV and upload to GCS"
depends: []

@bruin """
import os
import requests
from google.cloud import storage

start_date  = os.environ.get("BRUIN_START_DATE", "2018-01-01")
year_month  = start_date[:7].replace("-", "")  # YYYYMM
bucket_name = os.environ["GCS_BUCKET_NAME"]
destination = f"raw/{year_month}_Generation_MD.csv"

url = (
    "https://www.emi.ea.govt.nz/Wholesale/Datasets/Generation/"
    f"Generation_MD/{year_month}_Generation_MD.csv"
)

print(f"Downloading {url}")
response = requests.get(url, timeout=60)
response.raise_for_status()

client = storage.Client()
bucket = client.bucket(bucket_name)
blob   = bucket.blob(destination)

if blob.exists():
    print(f"Already exists: gs://{bucket_name}/{destination}, skipping upload")
else:
    blob.upload_from_string(response.content, content_type="text/csv")
    print(f"Uploaded {len(response.content):,} bytes → gs://{bucket_name}/{destination}")
