from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class ProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: str
    restaurant_id: int
    active_from: datetime
    active_to: datetime


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id
                        ,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'_id' as product_id
                        ,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'name' as product_name
                        ,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'price' as product_price
                        ,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'name' as product_name
                        ,id::int as restaurant_id
                        ,object_id
                        ,update_ts as active_from
                        ,'2099-12-31 00:00:00.000'::timestamp as active_to
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:

            cur.execute(
                """
                    Update dds.dm_products set active_to = %(active_from)s where product_id = %(product_id)s;
                """,
                {
                    "active_from": product.active_from,
                    "product_id": product.product_id
                },)

            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id,product_name,product_price,restaurant_id,active_from,active_to)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(restaurant_id)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        restaurant_id = EXCLUDED.restaurant_id,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "restaurant_id": product.restaurant_id,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )


class ProductLoader:
    WF_KEY = "example_products_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.stg = ProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.stg.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
