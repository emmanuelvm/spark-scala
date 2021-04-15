import logging
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcSparkOperator, DataprocClusterCreateOperator, DataprocClusterDeleteOperator  
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

log.info("datetime.today(): " + str(datetime.today()))
log.info("datetime.now(): " + str(datetime.now()))


ENVIRONMENT_GCP = Variable.get('RESERVAS-ENVIRONMENT_GCP')
log.info("LOG DE ENVIRONMENT_GCP: " + ENVIRONMENT_GCP)

# ---------------------- PROJECT ------------------------------
PROJECT_ID = Variable.get('RESERVAS-PROJECT_ID')
log.info("LOG DE PROJECT_ID: " + PROJECT_ID)
NAME_BUSINESS_PROJECT = 'VDG_RESERVAS'
NAME_PRODUCT_TECH = 'google_composer'

# ---------------------- DAG ----------------------------------
NAME_DAG_MAIN = 'DAG_' + NAME_BUSINESS_PROJECT

SCHEDULE_INTERVAL = Variable.get('RESERVAS-SCHEDULE_INTERVAL')
log.info("LOG DE SCHEDULE_INTERVAL: " + SCHEDULE_INTERVAL)

# ---------------------- CLUSTER ------------------------------
dataprocClusterOptions = Variable.get("RESERVAS-DATAPROC_CLUSTER_OPTIONS", deserialize_json=True)

CLUSTER_NAME = dataprocClusterOptions['RESERVAS-CLUSTER_NAME']
log.info("LOG DE CLUSTER_NAME: " + CLUSTER_NAME)

SERVICE_ACCOUNT = dataprocClusterOptions['RESERVAS-SERVICE_ACCOUNT']
log.info("LOG DE SERVICE_ACCOUNT: " + SERVICE_ACCOUNT)

REGION_NAME = dataprocClusterOptions['RESERVAS-REGION_NAME']
log.info("LOG DE REGION_NAME: " + REGION_NAME)

ZONE_NAME = dataprocClusterOptions['RESERVAS-ZONE_NAME']
log.info("LOG DE ZONE_NAME: " + ZONE_NAME)

SUBNET_NAME = dataprocClusterOptions['RESERVAS-SUBNET_NAME']
log.info("LOG DE SUBNET_NAME: " + SUBNET_NAME)

STORAGE_BUCKET_CLUSTER = dataprocClusterOptions['RESERVAS-STORAGE_BUCKET_CLUSTER']
log.info("LOG DE STORAGE_BUCKET_CLUSTER: " + STORAGE_BUCKET_CLUSTER)

# ---------------------- NODE MASTER ----------------------------
NUM_MASTERS = dataprocClusterOptions['RESERVAS-NUM_MASTERS']
log.info("LOG DE NUM_MASTERS: " + NUM_MASTERS)

MASTER_MACHINE_TYPE = dataprocClusterOptions['RESERVAS-MASTER_MACHINE_TYPE']
log.info("LOG DE MASTER_MACHINE_TYPE: " + MASTER_MACHINE_TYPE)

MASTER_DISK_SIZE = dataprocClusterOptions['RESERVAS-MASTER_DISK_SIZE']
log.info("LOG DE MASTER_DISK_SIZE: " + MASTER_DISK_SIZE)

# ---------------------- NODES WORKERS ---------------------------
NUM_WORKERS = dataprocClusterOptions['RESERVAS-NUM_WORKERS']
log.info("LOG DE NUM_WORKERS: " + NUM_WORKERS)

WORKER_MACHINE_TYPE = dataprocClusterOptions['RESERVAS-MASTER_MACHINE_TYPE']
log.info("LOG DE WORKER_MACHINE_TYPE: " + WORKER_MACHINE_TYPE)

WORKER_DISK_SIZE = dataprocClusterOptions['RESERVAS-WORKER_DISK_SIZE']
log.info("LOG DE WORKER_DISK_SIZE: " + WORKER_DISK_SIZE)

# ---------------------- SETTINGS -------------------------------
IMAGE_VERSION = dataprocClusterOptions['RESERVAS-IMAGE_VERSION']
log.info("LOG DE IMAGE_VERSION: " + IMAGE_VERSION)

IDLE_DELETE_TTL = dataprocClusterOptions['RESERVAS-IDLE_DELETE_TTL']
log.info("LOG DE IDLE_DELETE_TTL: " + IDLE_DELETE_TTL)

MAX_TO_STRING_FIELDS = Variable.get('RESERVAS-MAX_TO_STRING_FIELDS')
log.info("LOG DE MAX_TO_STRING_FIELDS: " + MAX_TO_STRING_FIELDS)

LOGGING_ENABLE = Variable.get('RESERVAS-LOGGING_ENABLE')
log.info("LOG DE LOGGING_ENABLE: " + LOGGING_ENABLE)

CLUSTER_PROPERTIES = {"spark:spark.debug.maxToStringFields": MAX_TO_STRING_FIELDS,
                      "spark:spark.sql.debug.maxToStringFields": MAX_TO_STRING_FIELDS,
                      "dataproc:dataproc.logging.stackdriver.job.driver.enable": LOGGING_ENABLE,
                      "dataproc:dataproc.logging.stackdriver.enable": LOGGING_ENABLE,
                      "dataproc:jobs.file-backed-output.enable": LOGGING_ENABLE,
                      "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": LOGGING_ENABLE}


log.info("LOG DE RESERVAS-CLUSTER_PROPERTIES: " + Variable.get("RESERVAS-CLUSTER_PROPERTIES"))
dataprocClusterProperties = Variable.get("RESERVAS-CLUSTER_PROPERTIES", deserialize_json=True)

for key, value in dataprocClusterProperties.items():
    CLUSTER_PROPERTIES[key] = value

properties = "\n"
for key, value in CLUSTER_PROPERTIES.items():
    properties = properties + key + " : " + value + "\n"
log.info("LOG DE CLUSTER_PROPERTIES: " + properties)

# ---------------------- COMPONENTES ----------------------------
BUCKET_DATAPROC_SPARK_JARS = Variable.get('RESERVAS-BUCKET_DATAPROC_SPARK_JARS')
log.info("LOG DE BUCKET_DATAPROC_SPARK_JARS: " + BUCKET_DATAPROC_SPARK_JARS)

# SPARK_SUBMIT_PROPERTIES_AVRO = {'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.5'}
SPARK_SUBMIT_PROPERTIES = {'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.5'}

BIG_QUERY_JAR = "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
NAMESPACE_MAIN_CLASS = 'mx.com.gnp.dlk.vida'

# PREFIX_COMPONENT = 'DLK_HDP_VDI_'
PREFIX_COMPONENT = 'GNP_GDP_VDG_'

PLATFORM = 'GCP'
SIS_ORI_INFO = 'INFO'
SIS_ORI_EVO = 'EVO'
SIS_ORI_PLNK = 'PLNK'

COMPONENT_CATALOGOS_GENERALES = PREFIX_COMPONENT + 'CATALOGOS_GENERALES'
COMPONENT_POLIZAS_CERTIFICADO = PREFIX_COMPONENT + 'POLIZAS_CERTIFICADO'
COMPONENT_SINIESTROS = PREFIX_COMPONENT + 'SINIESTROS'

COMPONENT_RPT_SINIESTROS_PERSONAS_MORALES = PREFIX_COMPONENT + 'RPT_SINIESTROS_PERSONAS_MORALES'

# ---------------------- BIG QUERY ----------------------------
bigQueryOptions = Variable.get("RESERVAS-BIG_QUERY_OPTIONS", deserialize_json=True)

SOURCE_BUCKET = bigQueryOptions['RESERVAS-SOURCE_BUCKET']
log.info("LOG DE SOURCE_BUCKET: " + SOURCE_BUCKET)

SOURCE_OBJECTS = bigQueryOptions['RESERVAS-SOURCE_OBJECTS']
log.info("LOG DE SOURCE_OBJECTS: " + SOURCE_OBJECTS)

PROJECT_ID_BQ = bigQueryOptions['RESERVAS-PROJECT_ID_BQ']
log.info("LOG DE PROJECT_ID_BQ: " + PROJECT_ID_BQ)

DATASET_BQ = bigQueryOptions['RESERVAS-DATASET_BQ']
log.info("LOG DE DATASET_BQ: " + DATASET_BQ)

TABLE_NAME_BQ = bigQueryOptions['RESERVAS-TABLE_NAME_BQ']
log.info("LOG DE TABLENAME_BQ: " + TABLE_NAME_BQ)

default_dag_args = {
    'owner': NAME_BUSINESS_PROJECT,
    'start_date': datetime.strptime("2020-10-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
    # 'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        NAME_DAG_MAIN,
        schedule_interval=SCHEDULE_INTERVAL,
        catchup=False,
        default_args=default_dag_args) as DAG_VDG_RESERVAS:

    dpcco_vdg_create_cluster = DataprocClusterCreateOperator(
        task_id='dpcco_vdg_create_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        num_masters=NUM_MASTERS,
        master_machine_type=MASTER_MACHINE_TYPE,
        master_disk_size=MASTER_DISK_SIZE,
        num_workers=NUM_WORKERS,
        worker_machine_type=WORKER_MACHINE_TYPE,
        worker_disk_size=WORKER_DISK_SIZE,
        image_version=IMAGE_VERSION,
        idle_delete_ttl=IDLE_DELETE_TTL,
        zone=ZONE_NAME,
        subnetwork_uri=SUBNET_NAME,
        storage_bucket=STORAGE_BUCKET_CLUSTER,
        properties=CLUSTER_PROPERTIES,
        service_account=SERVICE_ACCOUNT,
        # service_account_scopes = ['https://www.googleapis.com/auth/cloud-platform'],
        tags=['dataproc', PROJECT_ID],
        labels={'dataproc_cluster': CLUSTER_NAME,
                'name_project': NAME_BUSINESS_PROJECT.lower(),
                'project_id': PROJECT_ID}
    )

    # dpso_catalogos_generales = DataProcSparkOperator(
    #     task_id = 'dpso_catalogos_generales',
    #     cluster_name = CLUSTER_NAME,
    #     dataproc_spark_properties = SPARK_SUBMIT_PROPERTIES,
    #     dataproc_spark_jars = [BUCKET_DATAPROC_SPARK_JARS + COMPONENT_CATALOGOS_GENERALES + '.jar'],
    #     main_class = NAMESPACE_MAIN_CLASS + '.' + COMPONENT_CATALOGOS_GENERALES,
    #     arguments = [PLATFORM, ENVIRONMENT_GCP, SIS_ORI_INFO]
    # )

    dpso_vdg_polizas_certificado = DataProcSparkOperator(
        task_id = 'dpso_vdg_polizas_certificado',
        cluster_name = CLUSTER_NAME,
        dataproc_spark_properties = SPARK_SUBMIT_PROPERTIES,
        dataproc_spark_jars = [BUCKET_DATAPROC_SPARK_JARS + COMPONENT_POLIZAS_CERTIFICADO + '.jar'],
        main_class = NAMESPACE_MAIN_CLASS + '.' + COMPONENT_POLIZAS_CERTIFICADO,
        arguments = [PLATFORM, ENVIRONMENT_GCP, SIS_ORI_INFO]
    )

    dpso_vdg_siniestros = DataProcSparkOperator(
        task_id = 'dpso_vdg_siniestros',
        cluster_name = CLUSTER_NAME,
        dataproc_spark_properties = SPARK_SUBMIT_PROPERTIES,
        dataproc_spark_jars = [BUCKET_DATAPROC_SPARK_JARS + COMPONENT_SINIESTROS + '.jar'],
        main_class = NAMESPACE_MAIN_CLASS + '.' + COMPONENT_SINIESTROS,
        arguments = [PLATFORM, ENVIRONMENT_GCP, SIS_ORI_INFO]
    )

    dpso_vdg_rpt_siniestros_personas_morales = DataProcSparkOperator(
        task_id = 'dpso_vdg_rpt_siniestros_personas_morales',
        cluster_name = CLUSTER_NAME,
        dataproc_spark_properties = SPARK_SUBMIT_PROPERTIES,
        dataproc_spark_jars = [BUCKET_DATAPROC_SPARK_JARS + COMPONENT_RPT_SINIESTROS_PERSONAS_MORALES + '.jar'],
        main_class = NAMESPACE_MAIN_CLASS + '.' + COMPONENT_RPT_SINIESTROS_PERSONAS_MORALES,
        arguments = [PLATFORM, ENVIRONMENT_GCP, SIS_ORI_INFO]
    )

    bo_borrar_bq_vdg_rpt_siniestros_personas_morales = BashOperator(
        task_id="bo_borrar_bq_vdg_rpt_siniestros_personas_morales",
        bash_command="bq rm -f -t " + PROJECT_ID_BQ + ":" + DATASET_BQ + "." + TABLE_NAME_BQ
    )

    gcs2bqo_crear_bq_vdg_rpt_siniestros_personas_morales = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs2bqo_crear_bq_vdg_rpt_siniestros_personas_morales',
        bucket = SOURCE_BUCKET,
        source_objects = [SOURCE_OBJECTS],
        destination_project_dataset_table = PROJECT_ID_BQ + "." + DATASET_BQ + "." + TABLE_NAME_BQ,
        source_format = 'PARQUET',
        write_disposition = 'WRITE_TRUNCATE'
    )

    dpcdo_vdg_delete_cluster = DataprocClusterDeleteOperator(
        task_id = 'dpcdo_vdg_delete_cluster',
        cluster_name = CLUSTER_NAME,
        project_id = PROJECT_ID,
        depends_on_past = False,
        trigger_rule = TriggerRule.ALL_DONE
    )

    END_DAG = DummyOperator(
        task_id='FIN',
        depends_on_past = False
    )

dpcco_vdg_create_cluster.dag = DAG_VDG_RESERVAS

dpcco_vdg_create_cluster \
    >> [dpso_vdg_polizas_certificado, dpso_vdg_siniestros] \
    >> dpso_vdg_rpt_siniestros_personas_morales \
    >> bo_borrar_bq_vdg_rpt_siniestros_personas_morales >> gcs2bqo_crear_bq_vdg_rpt_siniestros_personas_morales \
    >> dpcdo_vdg_delete_cluster \
    >> END_DAG