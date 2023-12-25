from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators import dataproc
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


# Fetching Dynamically accepted parameters
dynamic_configs = Variable.get("input_parameters", deserialize_json=True)
CLUSTER_NAME = dynamic_configs.get("CLUSTER_NAME")
PROJECT_ID = dynamic_configs.get("PROJECT_ID")
REGION = dynamic_configs.get("REGION")
CLUSTER_CONFIG = dynamic_configs.get("CLUSTER_CONFIG")
spark_main_file = dynamic_configs.get("spark_main_file")
output_hive_database = dynamic_configs.get("output_hive_database")
output_hive_table = dynamic_configs.get("output_hive_table")


if CLUSTER_CONFIG is None:
    CLUSTER_CONFIG = {
        "master_config":{
            "num_instances":1,
            "machine_type_uri":"n1-standard-2",
            "disk_config":{
                "boot_disk_type":"pd-standard",
                "boot_disk_size_gb":50
            }
        },
        "worker_config":{
            "num_instances":2,
            "machine_type_uri":"n1-standard-2",
            "disk_config":{
                "boot_disk_type":"pd-standard",
                "boot_disk_size_gb":50
            }
        },
        "software_config":{
            "image_version":"2.1-debian11"
        }
    }

if spark_main_file is None:
    spark_main_file = "gs://project-airflow/assignment_spark_job.py"

pyspark_job_arguments = { "spark_main_file":spark_main_file}


default_args = {
    "owner":"amitS",
    "depends_on_past":False,
    "email_on_failure":True,
    "emails":["sangwanamit@gmail.com"],
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(minutes=3),
    "schedule_interval": "@daily",
    "start_date": days_ago(1),
    "catchup":False
}

with DAG("assignment1", default_args=default_args):
    
    create_cluster = dataproc.DataprocCreateClusterOperator(
        task_id="cluster_creation",
        project_id= PROJECT_ID,
        cluster_name= CLUSTER_NAME,
        region= REGION,
        cluster_config= CLUSTER_CONFIG,
    )
    
    check_books_files =GCSObjectsWithPrefixExistenceSensor(
        task_id="books_files_existance",
        bucket = "project-airflow",
        prefix = "input_data/books/books*.csv",
        poke_interval=300,
        timeout=60*60*12
    )

    check_orders_files = GCSObjectsWithPrefixExistenceSensor(
        task_id = "orders_files_existance",
        bucket = "project-airflow",
        prefix = "input_data/orders/orders*.csv",
        poke_interval=300,
        timeout=60*60*12
    )

    submit_spark_job = dataproc.DataprocSubmitPySparkJobOperator(
        task_id = "submit_spark_job",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME,
        main = pyspark_job_arguments["spark_main_file"],
        arguments = [f"--output_hive_database={output_hive_database}",
                     f"--output_hive_table={output_hive_table}"]
    )
    
    archive_files_books = GCSToGCSOperator(
        task_id="archive_books_file_after_transformation",
        source_bucket="project-airflow",
        source_object="input_data/books/*.csv",
        destination_bucket="project-airflow",
        destination_object="archived/books/",
        move_object=True
    )
    
    archive_files_orders = GCSToGCSOperator(
        task_id="archive_orders_file_after_transformation",
        source_bucket="project-airflow",
        source_object="input_data/orders/*.csv",
        destination_bucket="project-airflow",
        destination_object="archived/orders/",
        move_object=True
    )

    delete_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id="cluster_deletion",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE
    )
    

    [check_books_files, check_orders_files] >> create_cluster >> submit_spark_job >> [archive_files_books, archive_files_orders] >> delete_cluster
    
