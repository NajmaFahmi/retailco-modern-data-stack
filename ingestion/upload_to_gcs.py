"""Ingestion job: convert raw sales CSV to Parquet and upload to the GCS Bronze layer.

This is the first stage of the RetailCo modern data stack pipeline.
The Bronze layer stores immutable raw data as Parquet for downstream Spark processing.
"""

import pandas as pd
from google.cloud import storage


##### Define GCS Configuration
BUCKET_NAME = "retailco-raw-najma"
LOCAL_CSV = "data/raw/sales.csv"
LOCAL_PARQUET = "data/raw/sales.parquet"
GCS_BRONZE_PATH = "retailco/bronze/sales/sales.parquet"


##### CONVERT CSV TO PARQUET FUNCTION
### 1. Read CSV Raw Data 
### 2. Convert CSV Into Parquet
def convert_csv_to_parquet(csv_path: str, parquet_path: str) -> None:
    """Read a CSV file and write it back out as a Parquet file."""
    print("Reading raw CSV...")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

    print("Converting CSV to Parquet...")
    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    print(f"Parquet file written to {parquet_path}")


##### UPLOAD PARQUET TO GCS FUNCTION
def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str) -> None:
    """Upload a local file to the given GCS bucket and object path."""
    print(f"Uploading to gs://{bucket_name}/{destination_blob}...")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print("Upload complete. Bronze layer is ready.")


##### Run Automatically (end to end)
def main() -> None:
    """Run the full ingestion job end to end."""
    convert_csv_to_parquet(LOCAL_CSV, LOCAL_PARQUET)
    upload_to_gcs(BUCKET_NAME, LOCAL_PARQUET, GCS_BRONZE_PATH)

if __name__ == "__main__":
    main()