from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class FctObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: str
    total_sum: str
    bonus_payment: str
    bonus_grant: str


class FctsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fcts(self, fct_threshold: int, limit: int) -> List[FctObj]:
        with self._db.client().cursor(row_factory=class_row(FctObj)) as cur:
            cur.execute(
                """
                    with fct as(
                        select 
                            id
                            ,object_value::json->>'_id' as order_id
                            ,json_array_elements(object_value::json->'order_items')->>'quantity' as count
                            ,json_array_elements(object_value::json->'order_items')->>'price' as price
                            ,json_array_elements(object_value::json->'order_items')->>'id' as product_id
                        --	,object_value::json->>'bonus_payment' as bonus_payment
                        --	,object_value::json->>'bonus_grant' as bonus_grant
                        from stg.ordersystem_orders o),
                        bonussystem as(
                        select
                            event_value::json->>'order_id' as order_id
                            ,json_array_elements(event_value::json->'product_payments')->>'product_id' as product_id
                            ,json_array_elements(event_value::json->'product_payments')->>'bonus_payment' as bonus_payment
                            ,json_array_elements(event_value::json->'product_payments')->>'bonus_grant' as bonus_grant
                        from stg.bonussystem_events be
                        where event_type = 'bonus_transaction'
                        ),
                        pre_agg as(
                        select 
                            fct.id
                            ,dp.id as product_id
                            ,o.id as order_id
                            ,count::int
                            ,price::numeric(19,5)
                            ,(count::int * price::numeric(19, 5))::numeric(19, 5) as total_sum
                            ,b.bonus_payment::numeric(19,5)
                            ,b.bonus_grant::numeric(19,5)
                        from fct
                        join dds.dm_products dp on dp.product_id = fct.product_id
                        join dds.dm_orders o on o.order_key = fct.order_id
                        join bonussystem b on b.order_id = fct.order_id and b.product_id = fct.product_id)
                        select
                            id
                            ,product_id
                            ,order_id
                            ,count
                            ,price
                            ,total_sum
                            ,bonus_payment
                            ,bonus_grant
                        from pre_agg
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": fct_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctDestRepository:

    def insert_fct(self, conn: Connection, fct: FctObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id,order_id,count,price,total_sum,bonus_payment,bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s,%(price)s,%(total_sum)s,%(bonus_payment)s,%(bonus_grant)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": fct.product_id,
                    "order_id": fct.order_id,
                    "count": fct.count,
                    "price": fct.price,
                    "total_sum": fct.total_sum,
                    "bonus_payment": fct.bonus_payment,
                    "bonus_grant": fct.bonus_grant
                },
            )


class FctLoader:
    WF_KEY = "example_ofcts_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctsOriginRepository(pg_origin)
        self.stg = FctDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fcts(self):
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
            load_queue = self.origin.list_fcts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fcts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct in load_queue:
                self.stg.insert_fct(conn, fct)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
