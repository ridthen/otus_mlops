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
YC_DP_SSH_PUBLIC_KEY = 'ssh key'
YC_DP_SUBNET_ID = 'subnet id'
YC_DP_GROUP_ID = 'security group'
YC_DP_SA_ID = 'sa id'
YC_DP_METASTORE_URI = '10.128.0.24'
YC_DP_AZ = 'ru-central1-a'
YC_SOURCE_BUCKET = 'pyspark-bucket'
YC_DP_LOGS_BUCKET = 'fraudnames-log-bucket'

session = settings.Session()
ycS3_connection = Connection(
    conn_id='yc-s3'
)
if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

ycSA_connection = Connection(
    conn_id='yc-airflow-sa'
)
if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
    session.add(ycSA_connection)
    session.commit()

with DAG(
        'VALIDATE',
        schedule_interval='@once',
        start_date=dt.datetime.now(),
        tags=['data-proc-and-airflow'],
        max_active_runs=1,
        catchup=False
) as ingest_dag:

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
            'pip:mlflow' : '',
            'pip:urllib3' : '1.26',
            'pip:matplotlib' : '',
            'pip:seaborn' : '0.13.2',
            'pip:pandas' : '',
        },
        security_group_ids=[YC_DP_GROUP_ID],
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/staging.py',
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=ingest_dag
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
