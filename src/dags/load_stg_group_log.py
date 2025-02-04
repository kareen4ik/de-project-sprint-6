import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import vertica_python
import logging

log = logging.getLogger(__name__)

def load_group_log_to_vertica(**kwargs):
    """
    Загружаем данные в Vertica через COPY FROM STDIN, иначе permission denied
    """

    conn_info = {
        'host': 'vertica.tgcloudenv.ru',
        'port': 5433,
        'user': 'stv2025011438',
        'password': 'Z2bPBAdw54bdJ6b',
        'database': 'stv2025011438',
        'autocommit': True,
    }

    file_path = os.path.join(os.getcwd(), "data", "group_log_prepared.csv")

    if not os.path.exists(file_path):
        log.error(f"Файл {file_path} не найден!!!")
        raise FileNotFoundError(f"Файл {file_path} не найден!!!!")

    with open(file_path, 'r', encoding='utf-8') as f:
        next(f)
        try:
            with vertica_python.connect(**conn_info) as connection:
                cur = connection.cursor()

                cur.copy(
                    """
                    COPY STV2025011438__STAGING.group_log 
                    FROM STDIN
                    DELIMITER ','
                    ENCLOSED BY '"'
                    """,
                    f
                )

                log.info("Данные успешно загружены в Vertica!")
        
        except Exception as e:
            log.error(f"Ошибка при загрузке данных: {e}")
            raise

default_args = {
    'start_date': datetime(2004, 1, 1),
    'catchup': False,
}


with DAG(
    dag_id='load_group_log_to_vertica',
    default_args=default_args,
    schedule_interval='@once',
    description='Загрузка данных из group_log.csv в STV2025011438__STAGING.group_log',
    catchup=False,
) as dag:
    
    load_group_log_task = PythonOperator(
        task_id='load_group_log',
        python_callable=load_group_log_to_vertica
    )

    load_group_log_task
