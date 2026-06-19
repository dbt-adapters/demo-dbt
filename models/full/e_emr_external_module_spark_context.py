def model(dbt, spark_session):
    dbt.config(
        materialized="table",
        submission_method="emr_serverless",  # default is "athena_helper"
        spark_properties={
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",
        },
    )
    sc = spark_session.sparkContext
    sc.addPyFile("s3://snk-demo-dev/library/pymodules/file1.py")
    """
    file1.py
    ---------------------------
    def xyz(input):
        return 'xyz  - udf ' + str(input)
    """

    sc.addPyFile("s3://snk-demo-dev/library/pymodules/file2.py")
    """
    file2.py
    ---------------------------
    from file1 import xyz
    
    def uvw(input):
        return 'uvw -> ' + xyz(input)  
    """

    def val_transform(val):
        """
        import required function from external module
        """
        from file2 import uvw
        return uvw(val)



    a = val_transform("a")
    data = [(1, a), (2, "b"), (3, "c")]
    cols = ["num", "alpha"]
    df = spark_session.createDataFrame(data, cols)

    return df
