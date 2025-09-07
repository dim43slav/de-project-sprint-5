import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.project_stg_api_load.stg_api_load import Loader
from lib import ConnectionBuilder
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'schema', 'ddl', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_stg_load_dag():
    # Создаем подключение к базе dwh.
    #dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    dwh_pg_connect = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    # Объявляем таск, который создает структуру таблиц.
    @task(task_id="load_delivery")
    def stg_load_delivery():
        print('ad')
        a = Loader()
        a.get_restaurants(dwh_pg_connect)

    @task(task_id="load_couriers")
    def stg_load_couriers():
        print('ad')
        a = Loader()
        a.get_couriers(dwh_pg_connect)

    # Инициализируем объявленные таски.
    stg_load_delivery = stg_load_delivery()
    stg_load_couriers = stg_load_couriers()

    # Задаем последовательность выполнения тасков. У нас только инициализация схемы.
    stg_load_delivery >> stg_load_couriers

stg_api_load_dag = sprint5_project_stg_load_dag()