import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dds_users_loader import UserLoader
from examples.dds.dds_restaurants_loader import RestaurantLoader
from examples.dds.dds_timestamps_loader import TimestampLoader
from examples.dds.dds_products_loader import ProductLoader
from examples.dds.dds_orders_loader import OrderLoader
from examples.dds.dds_fcts_loader import FctLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.
    users_dict = load_dm_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.
    restaurants_dict = load_dm_restaurants()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.
    timestamps_dict = load_dm_timestamps()


    @task(task_id="dm_products_load")
    def load_dm_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.
    products_dict = load_dm_products()

    @task(task_id="dm_orders_load")
    def load_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.
    orders_dict = load_dm_orders()

    @task(task_id="dm_fcts_load")
    def load_dm_fcts():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = FctLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_fcts()  # Вызываем функцию, которая перельет данные.
    fcts_dict = load_dm_fcts()


    users_dict>>restaurants_dict>>timestamps_dict>>products_dict>>orders_dict>>fcts_dict


stg_bonus_system_users_dag = sprint5_dds_load_dag()
