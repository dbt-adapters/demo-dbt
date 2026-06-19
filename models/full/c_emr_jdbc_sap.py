def model(dbt, spark_session):
    dbt.config(
        tags=["pii", "monthly"],
        submission_method="emr_serverless",  # default is "athena_helper"
        materialized="table",
        table_type="iceberg",
        on_schema_change="append_new_columns",
        format="parquet",
        polling_interval=5,  # in seconds
        timeout=300,  # time out in seconds
        table_properties={"format-version": "2"},
        spark_properties={
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",
            "spark.jars": "s3://snk-demo-dev/library/jars/*.jar",
        },
    )

    jdbc_url = dbt.config.get("sh_dev_url", "")
    connection_properties = {
        "user": dbt.config.get("sh_dev_user", ""),
        "password": dbt.config.get("sh_dev_password", ""),
        "driver": "com.sap.db.jdbc.Driver",
    }

    query = "(SELECT * FROM ADR2 LIMIT 5) AS a"

    # Reading the data from SAP HANA
    df = spark_session.read.jdbc(
        url=jdbc_url, table=query, properties=connection_properties
    )
    return df
