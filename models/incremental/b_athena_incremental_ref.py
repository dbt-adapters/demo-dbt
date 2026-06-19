from pyspark.sql.functions import current_timestamp

def model(dbt, spark_session):
    dbt.config(
        tags=["athena", "daily"],
        materialized="table",
        polling_interval=5,  # in seconds
        timeout=300,  # time out in seconds
    )
    df_ref = dbt.ref("a_athena_full")

    df = df_ref.withColumn("dl_load_date", current_timestamp())
    if dbt.is_incremental:
        max_from_this = (
            f"select max(id) from {dbt.this.schema}.{dbt.this.identifier}"
        )
        df = df.filter(df.run_date >= spark_session.sql(max_from_this).collect()[0][0])

    return df
