"""Data quality validation for the RetailCo Bronze layer.

Uses Great Expectations 1.x to validate raw sales data before Spark processing.
If any expectation fails, the script exits with code 1 to halt the pipeline,
preventing bad data from flowing into Silver, BigQuery, and dbt.
"""

import sys
import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
)


### Configuration
BUCKET = "retailco-raw-najma"
BRONZE_PATH = f"gs://{BUCKET}/retailco/bronze/sales/sales.parquet"


## Define Valid Data
VALID_CITIES = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]
VALID_PRODUCTS = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam"]


## Function to Define Expectation
def build_expectation_suite(context) -> gx.ExpectationSuite:
    """Define and register all data quality expectations for the Bronze sales data."""

    # 1. create expectation suite
    suite = context.suites.add(gx.ExpectationSuite(name="sales_bronze_suite"))

    # 2. define expectations
    # order_id must not be null — every transaction needs an identifier
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="order_id"))

    # order_id must be unique — no duplicate transactions allowed
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="order_id"))

    # amount must not be null
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="amount"))

    # amount must be strictly positive — no zero or negative prices
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="amount", min_value=0, strict_min=True
    ))

    # quantity must be strictly positive
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="quantity", min_value=0, strict_min=True
    ))

    # city must belong to the known set of valid cities
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="city", value_set=VALID_CITIES
    ))

    # product must belong to the known set of valid products
    suite.add_expectation(ExpectColumnValuesToBeInSet(
            column="product", value_set=VALID_PRODUCTS
    ))

    return suite


## Main Function, Run End to End Pipeline
def main() -> None:
    """Read the Bronze layer, run GE validation, and halt on failure."""

    ## 1. Read Data
    print(f"Reading Bronze data from {BRONZE_PATH}...")
    df = pd.read_parquet(BRONZE_PATH)
    print(f"Loaded {len(df)} rows.")

    ## 2. Build an Ephemeral Data Context
    context = gx.get_context(mode="ephemeral")

    ## 3. Register the DataFrame as a GE data source
    data_source = context.data_sources.add_pandas("bronze_sales")
    data_asset = data_source.add_dataframe_asset("sales")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")

    ## 4. Build the Expectation Suite
    suite = build_expectation_suite(context)

    ## 5. Link the Suite to the Data Batch via Validation Definition
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="sales_bronze_validation",
            data=batch_definition,
            suite=suite,
        )
    )

    ## 6. Run the Validation
    result = validation_definition.run(batch_parameters={"dataframe": df})

    ## 7. Report Results
    print("\n=== GREAT EXPECTATIONS VALIDATION RESULTS ===")
    for expectation_result in result.results:
        status = "PASS" if expectation_result.success else "FAIL"
        expectation_type = expectation_result.expectation_config.type
        column = expectation_result.expectation_config.kwargs.get("column", "N/A")
        print(f"[{status}] {expectation_type} on column '{column}'")

    if not result.success:
        print("\nData quality checks FAILED. Stopping pipeline.")
        sys.exit(1)

    print("\nAll data quality checks passed. Safe to proceed.")



if __name__ == "__main__":
    main()
     