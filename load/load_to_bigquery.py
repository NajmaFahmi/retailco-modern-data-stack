"""Load job: GCS Silver layer to BigQuery.

Loads the cleaned and aggregated Parquet datasets from the GCS Silver layer
into BigQuery tables using native load (data is copied into BigQuery storage).
These tables become the source for downstream dbt modeling.
"""

from google.cloud import bigquery


### Configuration
PROJECT_ID = "najma-de-learning"
DATASET_ID = "retailco"
BUCKET = "retailco-raw-najma"

SILVER_CLEAN_URI = f"gs://{BUCKET}/{DATASET_ID}/silver/sales_clean/*.parquet"
SILVER_AGG_URI = f"gs://{BUCKET}/{DATASET_ID}/silver/sales_aggregated/*.parquet"

CLEAN_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sales_clean"
AGG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sales_aggregated"



### Function to Create BigQuery Dataset
def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """Create the BigQuery dataset if it does not already exist."""
    dataset_ref = f"{client.project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_ref}.")



### Function to Load Parquet Data into BigQuery Table
def load_parquet_to_table(client, source_uri, table_id):
    """Load Parquet files from GCS into a BigQuery table (overwrite mode)."""
    # cretae job configuration -- tell BigQuery how to load
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    # load parquet data into a bigquery table
    load_job = client.load_table_from_uri(
        source_uri, table_id, job_config=job_config
    )
    load_job.result()
    # read the metadata of the table
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}")



### Main Function, Run End to End Pipeline
def main() -> None:
    """Run the Silver to BigQuery load end to end."""
    ## 1. Define BigQuery Client
    client = bigquery.Client(project=PROJECT_ID)

    ## 2. Create Dataset (if not exist)
    ensure_dataset(client, DATASET_ID)

    ## 3. Load Parquet Data into BigQuery Table
    load_parquet_to_table(client, SILVER_CLEAN_URI, CLEAN_TABLE)
    load_parquet_to_table(client, SILVER_AGG_URI, AGG_TABLE)

    print("BigQuery load complete. Warehouse tables are ready.")



if __name__ == "__main__":
    main()
