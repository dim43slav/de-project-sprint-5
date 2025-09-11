import logging

import pendulum
from airflow.decorators import dag, task
from examples.dm.dm_courier_ledger import DmLoader
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dm_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_couriers():
        rest_loader = DmLoader()
        rest_loader.load_dm_couriers(dwh_pg_connect)
    dm_load = load_dm_couriers()



    dm_load


dm_dag = sprint5_dm_load_dag()
