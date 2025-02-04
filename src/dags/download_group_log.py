from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import os
import logging

def fetch_s3_file():
    """
    загружаем csv в /data/
    """
    AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
    AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # прописываю директорию, с ней побороться пришлось
    data_dir = "/data"
    file_path = os.path.join(data_dir, "group_log.csv")

    # если папки нет, её надо создать
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        logging.info(f"Создана директория: {data_dir}")

    bucket = "sprint6"
    key = "group_log.csv"

    try:
        logging.info(f"Начинаем загрузку s3://{bucket}/{key} в {file_path}")
        s3_client.download_file(Bucket=bucket, Key=key, Filename=file_path)
        logging.info(f"Файл успешно загружен в {file_path}")
    except Exception as e:
        logging.error(f"Ошибка загрузки файла: {e}")
        raise

def prepare_group_log_csv():
    """
    делаем преобразование user_id_from в Int64 
    """
    data_dir = "/data"
    csv_path = os.path.join(data_dir, "group_log.csv")

    # файл, найдись плиз (тут у меня были проблемы)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Файл {csv_path} не найден.")

    logging.info(f"Читаем CSV файл: {csv_path}")
    df_group_log = pd.read_csv(csv_path)
    df_group_log['user_id_from'] = pd.array(df_group_log['user_id_from'], dtype="Int64")

    # отдельно сохранм преобразованный файл, далее работать будем с ним
    prepared_csv_path = os.path.join(data_dir, "group_log_prepared.csv")
    df_group_log.to_csv(prepared_csv_path, index=False)
    logging.info(f"Преобразованный CSV сохранён: {prepared_csv_path}")

default_args = {
    'start_date': datetime(2004, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='download_files',
    schedule_interval='@once',
    default_args=default_args,
    description='Downloads group_log.csv from S3 and prepares it',
    catchup=False,
) as dag:

    download_log = PythonOperator(
        task_id='download_csv_files',
        python_callable=fetch_s3_file
    )

    prepare_task = PythonOperator(
        task_id='prepare_group_log_csv',
        python_callable=prepare_group_log_csv
    )

    download_log >> prepare_task
