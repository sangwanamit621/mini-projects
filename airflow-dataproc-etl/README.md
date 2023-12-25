# Airflow Dataproc ETL Pipeline 🚀

Welcome to the Airflow Dataproc ETL Pipeline mini project! This project is designed to showcase an end-to-end automated ETL pipeline using Apache Airflow, Google Cloud Dataproc, Google Cloud Storage, Hive, Spark, and Python. Dive into the world of orchestration and data processing with this hands-on example.

## Project Overview

This mini project demonstrates a robust ETL pipeline with the following key components:

- **GCSObjectsWithPrefixExistenceSensor**: Checks for the existence of files in a Google Cloud Storage bucket before triggering further processing.

- **DataprocCreateClusterOperator**: Spins up a Dataproc cluster dynamically to handle the Spark job.

- **DataprocSubmitPySparkJobOperator**: Submits a PySpark job to the Dataproc cluster, which reads data from the GCS bucket, performs data processing (joining and aggregation), and stores the final output in a Hive table.

- **GCSToGCSOperator**: After processing, moves the raw files in the GCS bucket to an archive folder within the same bucket.

- **DataprocDeleteClusterOperator**: Deletes the Dataproc cluster once the processing is complete.

## Getting Started

### Prerequisites

- [Google Cloud Platform](https://cloud.google.com/) account with access to Dataproc, GCS, and other necessary services.
- [Apache Airflow](https://airflow.apache.org/) installed and configured or we can use Composer which is a Google Managed Airflow.

### Project Setup

1. Clone this repository: `git clone https://github.com/your-username/airflow-dataproc-etl.git`
2. Navigate to the project directory: `cd airflow-dataproc-etl`

### Configuration

1. Configure your Airflow variables in the Airflow UI to provide dynamic input parameters in `input_parameters` key.
```python
{
    "CLUSTER_NAME": "spark-cluster",
    "PROJECT_ID": "spark-project-409004",
    "REGION": "us-central1",
    "CLUSTER_CONFIG": {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 50
            }
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 50
            }
        },
        "software_config": {
            "image_version": "2.1-debian11"
        }
    },
    "spark_main_file": "gs://project-airflow/assignment_spark_job.py",
    "output_hive_database": "books_order_process",
    "output_hive_table": "books_sold"
}
```


### Running the Pipeline

1. If running the self managed version, then start the Airflow scheduler and web server: `airflow webserver -p 8080` and `airflow scheduler`. 
2. Access the Airflow UI at `http://localhost:8080`.
3. Trigger the ETL pipeline DAG.
   
    __Note:__ Please make sure that the GCP service account have proper permissions to access the files as well as creating and deleting Dataproc cluster to aviod errors.

## Project Structure
```
airflow-dataproc-etl/
   |
   |--- README.md
   |--- dags/
   |     |
   |     |--- dataproc_etl_pipeline.py
   |
   |--- scripts/
   |     |--- spark_job.py
   |
   |--- input_data/
   |     |--- books/
   |     |--- orders/
   |

```

- **dags/**: Contains the Airflow DAG definition for the ETL pipeline.
- **scripts/**: Houses the PySpark job script for data processing.
- **input_data/**: Contains the input_data used to process

## Contributions and Feedback

Contributions, issues, and feedback are welcome! Feel free to open issues, submit pull requests, or share your experiences with this mini project. Let's learn and build together.

Happy ETL-ing in the cloud! 🌐☁️
