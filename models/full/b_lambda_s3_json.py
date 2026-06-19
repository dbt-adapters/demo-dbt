def model(dbt, spark_session):
    dbt.config(
        tags=["monthly"],
        materialized="table",
        table_type="iceberg",
        submission_method="lambda",
        on_schema_change="append_new_columns",
        format="parquet",
        polling_interval=5,  # in seconds
        timeout=100,  # time out in seconds
        table_properties={"format-version": "2"},
        spark_properties={
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",
        },
    )

    # S3 path to your JSON file
    s3_path = "s3://snk-demo-dev/sample/sample.json"

    # Read the JSON file from S3
    df = spark_session.read.option("multiLine", True).json(s3_path)

    return df
