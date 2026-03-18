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
table_id    = f"{project_id}.raw.generation_raw"

client = bigquery.Client(project=project_id)

try:
    client.get_table(table_id)
    print(f"External table {table_id} already exists, skipping")
except Exception:
    schema = [
        bigquery.SchemaField("Site_Code",    "STRING"),
        bigquery.SchemaField("POC_Code",     "STRING"),
        bigquery.SchemaField("Nwk_Code",     "STRING"),
        bigquery.SchemaField("Gen_Code",     "STRING"),
        bigquery.SchemaField("Fuel_Code",    "STRING"),
        bigquery.SchemaField("Tech_Code",    "STRING"),
        bigquery.SchemaField("Trading_Date", "STRING"),
    ] + [
        bigquery.SchemaField(f"TP{i}", "INTEGER") for i in range(1, 51)
    ]

    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [f"gs://{bucket_name}/raw/*_Generation_MD.csv"]
    external_config.skip_leading_rows = 1
    external_config.ignore_unknown_values = True
    external_config.schema = schema

    table = bigquery.Table(table_id, schema=schema)
    table.external_data_configuration = external_config

    client.create_table(table)
    print(f"Created external table {table_id}")
    print(f"  → source: gs://{bucket_name}/raw/*_Generation_MD.csv")
