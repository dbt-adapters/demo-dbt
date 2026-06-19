# This model doesnt work as Athena can only access external files via spark context
# and not via spark.submit.pyFiles
def model(dbt, spark_session):
    dbt.config(
        tags=["athena"],
        materialized="table",
        engine_config={
            "MaxConcurrentDpus": 2,
            "SparkProperties": {
                "spark.submit.pyFiles": "s3://snk-demo-dev/library/pymodules/file1.py,s3://snk-demo-dev/library/pymodules/file2.py",
            },
        },
    )

    def val_transform(val):
        """
        import required function from external module
        """
        from file2 import uvw # available via yaml spark_properties config
        return uvw(val)

    a = val_transform("a")
    data = [(1, a), (2, "b"), (3, "c")]
    cols = ["num", "alpha"]
    df = spark_session.createDataFrame(data, cols)

    return df
