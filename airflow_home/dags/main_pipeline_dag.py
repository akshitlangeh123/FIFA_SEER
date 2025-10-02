from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_HOME = "/opt/spark"
PROJECT_DIR = "/home/hadoop/FIFA_SEER"
VENV_PYSPARK = f"{PROJECT_DIR}/.venv/bin/activate"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG (
    dag_id = "fifa_pipeline",
    default_args = default_args,
    description = "pipeline (bronze -> silver -> write_to_hive -> cleanup)",
    schedule_interval = None,
    start_date = datetime(2025, 1, 1),
    catchup = False,
) as dag:

    bronze_task = BashOperator(
        task_id = "bronze_layer",
        bash_command = f"""
            source {VENV_PYSPARK} && \
            {SPARK_HOME}/bin/spark-submit \
            --master yarn \
            --num-executors 2 \
            --executor-cores 2 \
            --executor-memory 2G \
            --deploy-mode client \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
            {PROJECT_DIR}/bronze.py
        """
    )

    silver_task = BashOperator(
        task_id = "silver_Layer",
        bash_command = f"""
            source {VENV_PYSPARK} && \
            {SPARK_HOME}/bin/spark-submit \
            --master yarn \
            --num-executors 2 \
            --executor-cores 2 \
            --executor-memory 2G \
            --deploy-mode client \
            {PROJECT_DIR}/silver.py
        """
    )

    write_task = BashOperator(
        task_id = "write_to_hive",
        bash_command = f"""
            source {VENV_PYSPARK} && \
            {SPARK_HOME}/bin/spark-submit \
            --master yarn \
            --num-executors 2 \
            --executor-cores 2 \
            --executor-memory 2G \
            --deploy-mode client \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse \
            {PROJECT_DIR}/write_hive.py
        """
    )

    cleanup_task = BashOperator(
        task_id = "cleanup",
        bash_command = """
            hdfs dfs -rm -r hdfs://localhost:9000/user/hadoop/FIFA_SEER/tmp_data/bronze
            hdfs dfs -rm -r hdfs://localhost:9000/user/hadoop/FIFA_SEER/tmp_data/silver
        """
    )

    bronze_task >> silver_task >> write_task >> cleanup_task