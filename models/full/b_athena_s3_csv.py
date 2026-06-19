import time

import boto3
from pyspark.sql.functions import current_timestamp
import pyspark


athena_client = boto3.client('athena', region_name='us-east-1')


def run_athena_query(database, query, output_location):
    """
    Function to execute a query in Athena and wait for the result.
    """
    try:
        # Start the query execution
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']

        # Wait for the query to finish
        while True:
            status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status['QueryExecution']['Status']['State']

            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(2)  # Polling interval

        if state == 'SUCCEEDED':
            print("Query succeeded.")
        else:
            print(f"Query {state}: {status['QueryExecution']['Status']['StateChangeReason']}")
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']
        
        results = []
        if rows:
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            for row in rows[1:]:
                values = [col.get('VarCharValue', None) for col in row['Data']]
                results.append(dict(zip(headers, values)))
        return results

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def dq_check(dbt_model):
    """
    Decorator function to capture the data consistency check at each model
    :param dbt_model: python model function
    :return:
    """
    def wrapper(*args, **kwargs):
        dbt = args[0] if len(args) > 0 else kwargs.get("dbt", None)
        spark_session = args[1] if len(args) > 1 else kwargs.get("spark_session", None)

        df = dbt_model(*args, **kwargs)

        domain = f'{dbt.this.schema}'.split('_')[1]
        vendor = f'{dbt.this.schema}'.split('_')[-1]
        schema = f'snk_{domain}'
        audit_table_name = 'dq_audit'
        athena_output = f's3://snk-{domain}-dev/athena-query-results/'

        target_table = f"{dbt.this.schema}.{dbt.this.identifier}"

        target_count_query = f"""SELECT COUNT(*) as row_count from {target_table} where date(dl_load_date) = current_date"""
        target_count = run_athena_query(schema, target_count_query, athena_output)
        if target_count:
            target_count = target_count[0].get('row_count')
            print(f"Target Count = {target_count}")

        # source = df.inputFiles()
        # print(f"source location = {source}")

        log_data = (f"('manual_dag', '{vendor}', '{dbt.this.identifier}', {df.count()}, '{target_table}', "
                    f"current_timestamp, {target_count})")
        log_query = (f"INSERT INTO {schema}.{audit_table_name} "
                     f"(dag_name, vendor, model_name, source_count, target_table_name, audit_datetime, "
                     f"target_count) values {log_data}")

        run_athena_query(schema, log_query, athena_output)
        return df
    return wrapper



@dq_check
def model(dbt, spark_session):
    dbt.config(
        tags=["athena","monthly"],
        materialized="incremental",
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
    s3_path = "s3://snk-demo-dev/sample/sample.csv"

    # Read the JSON file from S3
    df = spark_session.read.csv(s3_path, header=True, inferSchema=True)
    df = df.withColumn('dl_load_date', current_timestamp())
    return df
