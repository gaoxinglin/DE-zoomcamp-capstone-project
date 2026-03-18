""" @bruin

name: raw.generation_raw
type: python
description: "Create BQ external table pointing to all monthly EMI CSVs in GCS"
depends: [raw.download_emi]

@bruin """
import os
from google.cloud import bigquery

project_id  = os.environ["GCP_PROJECT_ID"]
bucket_name = os.environ["GCS_BUCKET_NAME"]

client = bigquery.Client(project=project_id)

tp_columns = ",\n  ".join(f"TP{i} FLOAT64" for i in range(1, 51))

ddl = f"""
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.raw.generation_raw`
(
  Site_Code    STRING,
  POC_Code     STRING,
  Nwk_Code     STRING,
  Gen_Code     STRING,
  Fuel_Code    STRING,
  Tech_Code    STRING,
  Trading_Date STRING,
  {tp_columns}
)
OPTIONS (
  format                = 'CSV',
  skip_leading_rows     = 1,
  uris                  = ['gs://{bucket_name}/raw/*_Generation_MD.csv'],
  allow_jagged_rows     = true,
  ignore_unknown_values = true
)
"""

job = client.query(ddl)
job.result()
print(f"Created external table {project_id}.raw.generation_raw")
print(f"  → source: gs://{bucket_name}/raw/*_Generation_MD.csv")
