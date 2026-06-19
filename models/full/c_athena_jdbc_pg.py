def model(dbt, spark_session):
    dbt.config(
        tags=["athena", "daily"],
        materialized="table",
        # table_type="iceberg",
        on_schema_change="append_new_columns",
        format="parquet",
        polling_interval=5,  # in seconds
        timeout=100,  # time out in seconds
        # table_properties={"format-version": "2"},
        engine_config={
            "MaxConcurrentDpus": 2,
            "SparkProperties": {
                "spark.jars": "s3://snk-demo-dev/library/jars/postgresql-42.7.2.jar",
            },
        },
    )
    jdbc_url = dbt.config.get("pg_dev_url")
    jdbc_properties = {
        "user": dbt.config.get("pg_dev_user"),
        "password": dbt.config.get("pg_dev_pwd"),
        "driver": "org.postgresql.Driver",
    }
    # Table name to read
    table_name = "information_schema.tables"

    postgres_df = (
        spark_session.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", jdbc_properties["user"])
        .option("password", jdbc_properties["password"])
        .option("driver", jdbc_properties["driver"])
        .load()
    )
    return postgres_df
