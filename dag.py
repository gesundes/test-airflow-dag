from datetime import datetime
import os

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

print(os.environ["AIRFLOW_HOME"])

with DAG(
        dag_id='example_spark_operator_sdp',
        schedule_interval=None,
        start_date=datetime(2021, 1, 17),
        catchup=False,
        tags=['sdp_spark'],
) as dag:
    # [START howto_operator_spark_submit]
    submit_job = SparkSubmitOperator(
        application="dags/repo/tests/test_spark/pi.py",
        application_args=["10"],
        task_id="submit_job",
        conn_id="data_platform_spark",
        conf={"spark.pyspark.python": "/opt/anaconda3/bin/python3"}
    )
    # [END howto_operator_spark_submit]
