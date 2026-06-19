def model(dbt, spark_session):

    dbt.config(
        tags=["daily"],
        submission_method="emr_serverless",  # default is "athena_helper"
        materialized="table",
        polling_interval=5,  # in seconds
        timeout=300,  # time out in seconds
        spark_properties={
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",
            "spark.jars": "s3://snk-demo-dev/library/jars/*.jar",
        }
    )

    jdbc_url = f"jdbc:redshift://redshift-dev-warehouse.snk.net:5439/warehouse"

    jdbc_properties = {
        "user": dbt.config.get("rs_dev_user", ""),
        "password": dbt.config.get("rs_dev_password", ""),
        "driver": "com.amazon.redshift.jdbc.Driver",
    }

    table_name = "db.table_1"

    rs_df = (
        spark_session.read.format("jdbc")  # default jdbc other option is 'io.github.spark_redshift_community.spark.redshift' which would need iam auth and also issue with get credentials of boto3 it expects redshift and emr are running in same account
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", jdbc_properties["user"])
        .option("password", jdbc_properties["password"])
        .option("driver", jdbc_properties["driver"])
        .load()
    )

    return rs_df