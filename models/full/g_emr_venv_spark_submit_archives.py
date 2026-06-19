def model(dbt, spark_session):
    dbt.config(
        materialized="table",
        submission_method="emr_serverless",  # default is "athena_helper"
        spark_properties={
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.memory": "1g",
            "spark.archives": "s3://snk-demo-dev/library/spark-archives/emr_venv.tar.gz#environment",
            "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": "./environment/bin/python",
            "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": "./environment/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "./environment/bin/python",
            # "spark.executorEnv.PYTHONPATH": "./environment/lib/python3.9/site-packages:$PYTHONPATH",
            # "spark.emr-serverless.driverEnv.PYTHONPATH": "./environment/lib/python3.9/site-packages:$PYTHONPATH",
        },
    )

    import recordlinkage
    from recordlinkage.index import Full
    from recordlinkage.preprocessing import clean

    # Sample data
    data1 = [("Alice", "Smith", "25"), ("Bob", "Johnson", "30"), ("Charlie", "Smith", "35")]
    data2 = [("Alice", "Smyth", "26"), ("Robert", "Johnson", "30"), ("Charlie", "Smith", "36")]

    # Creating DataFrames
    df1 = spark_session.createDataFrame(data1, ["first_name", "last_name", "age"])
    df2 = spark_session.createDataFrame(data2, ["first_name", "last_name", "age"])

    # Convert Spark DataFrame to Pandas DataFrame for Record Linkage Toolkit
    df1_pd = df1.toPandas()
    df2_pd = df2.toPandas()

    # Clean data (optional, depending on need)
    df1_pd['first_name'] = clean(df1_pd['first_name'])
    df2_pd['first_name'] = clean(df2_pd['first_name'])

    # Initialize indexer object
    indexer = recordlinkage.Index()
    indexer.add(Full())
    candidate_links = indexer.index(df1_pd, df2_pd)

    # Initialize the comparison object
    compare_cl = recordlinkage.Compare()

    # Add comparisons using both Jaro-Winkler and Levenshtein
    compare_cl.string('first_name', 'first_name', method='jarowinkler', threshold=0.85, label='first_name_jarowinkler')
    compare_cl.string('first_name', 'first_name', method='levenshtein', threshold=0.85, label='first_name_levenshtein')
    compare_cl.string('last_name', 'last_name', method='jarowinkler', threshold=0.85, label='last_name_jarowinkler')
    compare_cl.string('last_name', 'last_name', method='levenshtein', threshold=0.85, label='last_name_levenshtein')
    compare_cl.exact('age', 'age', label='age')

    # Compute features
    features = compare_cl.compute(candidate_links, df1_pd, df2_pd)

    # Classification (optional)
    # Define a threshold for considering a match
    matches = features[features.sum(axis=1) >= 3]  # Adjust this threshold based on your criteria

    # Convert results back to Spark DataFrame
    result_df = spark_session.createDataFrame(matches.reset_index())
    return result_df