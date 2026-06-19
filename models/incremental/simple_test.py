def model(dbt, spark_session):
    dbt.config(
        tags=["athena", "monthly"],
        materialized="table",
        incremental_strategy="append",
        # table_type="iceberg",
        on_schema_change="append_new_columns",
        format="parquet",
        polling_interval=5,  # in seconds
        timeout=100,  # time out in seconds
        # table_properties={"format-version": "2"},
        engine_config={
            "MaxConcurrentDpus": 2,
        },
    )

    # S3 path to your csv file with headers
    s3_path = "s3://snk-demo-dev/sample/test123.csv"

    # Read the JSON file from S3
    df = spark_session.read.csv(s3_path, header=True, inferSchema=True)

    return df
