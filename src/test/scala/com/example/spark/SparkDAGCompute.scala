from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param, Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DAG_ID = "feat_calc_merge_dataproc"

PROJECT_ID = Variable.get("gcp_project_id")
REGION = Variable.get("gcp_region", default_var="us-central1")
ZONE = Variable.get("gcp_zone", default_var="us-central1-a")
SERVICE_ACCOUNT = Variable.get("dataproc_service_account")
SUBNETWORK_URI = Variable.get("dataproc_subnetwork_uri")

JAR_GCS_URI = Variable.get("feat_calc_jar_gcs_uri")
BIGLAKE_CATALOG = Variable.get("feat_calc_biglake_catalog")
WAREHOUSE_BUCKET = Variable.get("feat_calc_warehouse_bucket")  # gs://bucket
CATALOG_NAME = Variable.get("feat_calc_catalog_name", default_var="my_catalog")
TABLE_NAME = Variable.get("feat_calc_table_name", default_var="production.features")

CLUSTER_NAME_TEMPLATE = "feat-calc-{{ ds_nodash }}-{{ ts_nodash | lower }}"

# Iceberg + BigLake runtime packages.
ICEBERG_PACKAGES = ",".join(
    [
        # Spark 3.5 on Dataproc image 2.2; keep Scala 2.12 runtime.
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1",
        "org.apache.iceberg:iceberg-gcp-bundle:1.10.1",
    ]
)

# ---------------------------------------------------------------------------
# Cluster definition - sized for 250M row MERGE within a ~3 minute budget.
# ---------------------------------------------------------------------------

CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "zone_uri": f"projects/{PROJECT_ID}/zones/{ZONE}",
        "subnetwork_uri": SUBNETWORK_URI,
        "service_account": SERVICE_ACCOUNT,
        "internal_ip_only": True,
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform",
        ],
        "metadata": {
            # Pre-install Iceberg runtime so the job does not pay --packages
            # resolution cost on every submit.
            "SPARK_BQ_CONNECTOR_VERSION": "0.42.0",
        },
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-8",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 200},
    },
    "worker_config": {
        "num_instances": 8,
        "machine_type_uri": "n2-highmem-8",
        "disk_config": {
            "boot_disk_type": "pd-ssd",
            "boot_disk_size_gb": 500,
            "num_local_ssds": 2,
        },
    },
    "software_config": {
        "image_version": "2.2-debian12",
        "properties": {
            # Iceberg + BigLake catalog wiring. These values are baked into the
            # cluster so every submit inherits them.
            "spark:spark.jars.packages": ICEBERG_PACKAGES,
            "spark:spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark:spark.sql.catalog." + CATALOG_NAME: "org.apache.iceberg.spark.SparkCatalog",
            "spark:spark.sql.catalog." + CATALOG_NAME + ".catalog-impl": "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
            "spark:spark.sql.catalog." + CATALOG_NAME + ".gcp_project": PROJECT_ID,
            "spark:spark.sql.catalog." + CATALOG_NAME + ".gcp_location": REGION,
            "spark:spark.sql.catalog." + CATALOG_NAME + ".blms_catalog": BIGLAKE_CATALOG,
            "spark:spark.sql.catalog." + CATALOG_NAME + ".warehouse": WAREHOUSE_BUCKET,
            # Performance tuning for large MERGE workloads.
            "spark:spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark:spark.sql.adaptive.skewJoin.enabled": "true",
            "spark:spark.sql.shuffle.partitions": "400",
            "spark:spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark:spark.dynamicAllocation.enabled": "false",
        },
    },
    "lifecycle_config": {
        # Safety net: auto-delete idle cluster after 10 minutes if DAG crashes
        # before the delete task runs.
        "idle_delete_ttl": {"seconds": 600},
    },
}

# ---------------------------------------------------------------------------
# Spark job definition
# ---------------------------------------------------------------------------

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME_TEMPLATE},
    "spark_job": {
        "main_class": "com.example.spark.FeatCalcMergeApp",
        "jar_file_uris": [JAR_GCS_URI],
        "properties": {
            # App input path is read via this Spark conf key inside the job.
            "spark.featcalcmergeapp.parquetPath": "{{ params.parquet_path }}",
            # Redundant with cluster-level props but harmless and keeps the
            # job self-describing if the cluster config changes.
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl": "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
            f"spark.sql.catalog.{CATALOG_NAME}.gcp_project": PROJECT_ID,
            f"spark.sql.catalog.{CATALOG_NAME}.gcp_location": REGION,
            f"spark.sql.catalog.{CATALOG_NAME}.blms_catalog": BIGLAKE_CATALOG,
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE_BUCKET,
            # Driver / executor sizing (fits n2-highmem-8 with headroom).
            "spark.driver.memory": "8g",
            "spark.executor.memory": "24g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "16",
            "spark.executor.memoryOverhead": "4g",
        },
        "args": [
            # Pass the destination catalog/table as args in case the app is
            # later updated to consume them from argv.
            f"--catalog={CATALOG_NAME}",
            f"--table={TABLE_NAME}",
        ],
    },
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_ID,
    description="Ephemeral Dataproc run of FeatCalcMergeApp with BigLake-backed Iceberg catalog.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Triggered externally, once per input.
    catchup=False,
    max_active_runs=1,  # Serialize MERGE against the shared target table.
    tags=["spark", "iceberg", "dataproc", "biglake"],
    params={
        "parquet_path": Param(
            default="",
            type="string",
            title="Input Parquet GCS path",
            description="gs:// path of the input Parquet dataset to upsert.",
        ),
    },
    render_template_as_native_obj=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME_TEMPLATE,
        cluster_config=CLUSTER_CONFIG,
        use_if_exists=False,
    )

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=SPARK_JOB,
        # Fail fast - the SLA budget is tight.
        asynchronous=False,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME_TEMPLATE,
        trigger_rule=TriggerRule.ALL_DONE,  # Always tear down, even on failure.
    )

    create_cluster >> submit_spark_job >> delete_cluster
