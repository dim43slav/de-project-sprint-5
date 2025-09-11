from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class FctObj(BaseModel):
    id: int
    order_id: int
    courier_id: Optional[int] = None
    delivery_ts: Optional[str] = None
    address: Optional[str] = None
    rate: Optional[int] = None
    tip_sum: Optional[str] = None
    total_sum: str


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
                        from stg.ordersystem_orders o),
                        pre_agg as(
                        select 
                            fct.id
                            ,o.id as order_id
                            ,o.courier_id
                            ,d.delivery_ts
                            ,d.address
                            ,d.rate
                            ,d.tip_sum
                            ,sum(fct.count::numeric * fct.price::numeric) as total_sum
                        from fct
                        join dds.dm_orders o on o.order_key = fct.order_id
                        left join stg.deliveries d on d.order_id = o.order_key
                        group by fct.id
                            ,o.id
                            ,o.courier_id
                            ,d.delivery_ts
                            ,d.address
                            ,d.rate
                            ,d.tip_sum)
                        select
                            id
                            ,order_id
                            ,courier_id
                            ,delivery_ts::text
                            ,address
                            ,rate
                            ,tip_sum
                            ,total_sum
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
                    INSERT INTO dds.fct_deliveries (order_id,courier_id,delivery_ts,address,rate,tip_sum,total_sum)
                    VALUES (%(order_id)s, %(courier_id)s, %(delivery_ts)s,%(address)s,%(rate)s,%(tip_sum)s,%(total_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        delivery_ts = EXCLUDED.delivery_ts,
                        address = EXCLUDED.address,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        total_sum = EXCLUDED.total_sum;
                """,
                {
                    "order_id": fct.order_id,
                    "courier_id": fct.courier_id,
                    "delivery_ts": fct.delivery_ts,
                    "address": fct.address,
                    "rate": fct.rate,
                    "tip_sum": fct.tip_sum,
                    "total_sum": fct.total_sum
                },
            )


class FctLoader_delivery:
    WF_KEY = "example_ofcts_delivery_origin_to_dds_workflow"
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
