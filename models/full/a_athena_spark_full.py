def model(dbt, spark_session):
    dbt.config(
        tags=["athena","daily"],
        submission_method="athena_helper",  # default is "athena_helper"
        materialized="table",
        table_type="iceberg",
        on_schema_change="append_new_columns",
        format="parquet",
        polling_interval=5,  # in seconds
        timeout=300,  # time out in seconds
        table_properties={"format-version": "2"},
        engine_config={
            "MaxConcurrentDpus": 2
        },
    )

    data = [(1, "a"), (2, "b"), (3, "c")]
    cols = ["num", "alpha"]
    df = spark_session.createDataFrame(data, cols)

    return df
