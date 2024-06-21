import uuid
import datetime as dt
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_FOLDER_ID = 'folder id'
YC_DP_SSH_PUBLIC_KEY = 'spark public key'
YC_DP_SUBNET_ID = 'subnet id'
YC_DP_GROUP_ID = 'security group id'
YC_DP_SA_ID = 'service account id'
YC_DP_METASTORE_URI = 'metastore ip'
YC_DP_AZ = 'ru-central1-a'
YC_SOURCE_BUCKET = 'pyspark-bucket'
YC_DP_LOGS_BUCKET = 'fraudnames-log-bucket'

# Создание подключения для Object Storage
session = settings.Session()
ycS3_connection = Connection(
    conn_id='yc-s3'
)
if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

# Создание подключения для сервисного аккаунта
ycSA_connection = Connection(
    conn_id='yc-airflow-sa'
)
if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
    session.add(ycSA_connection)
    session.commit()

# Настройки DAG
with DAG(
        'DATA_INGEST',
        schedule_interval='0 */3 * * *',
        tags=['data-proc-and-airflow'],
        start_date=dt.datetime(2024, 6, 19),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    # 1 этап: создание кластера Yandex Data Proc
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        zone=YC_DP_AZ,
        cluster_image_version='2.0.43',
        enable_ui_proxy=False,
        masternode_resource_preset='s3-c2-m8',
        masternode_disk_type='network-hdd',
        masternode_disk_size=40,
        computenode_resource_preset='s3-c4-m16',
        computenode_disk_type='network-hdd',
        computenode_disk_size=128,
        computenode_count=3,
        computenode_max_hosts_count=3,  # Количество подкластеров для обработки данных будет автоматически масштабироваться в случае большой нагрузки.
        services=['YARN', 'SPARK'],     # Создается легковесный кластер.
        datanode_count=0,               # Без подкластеров для хранения данных.
        properties={                    # С указанием на удаленный кластер Metastore.
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
        security_group_ids=[YC_DP_GROUP_ID],
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag
    )

    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/dataclean.py',
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag
    )

    # 3 этап: удаление кластера Yandex Data Proc
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=ingest_dag
    )

    # Формирование DAG из указанных выше этапов
    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
