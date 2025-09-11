Список полей для витрины cdm.dm_courier_ledger

id	SERIAL	идентификатор записи.
courier_id	VARCHAR	ID ID курьера, которому перечисляем.
courier_name	VARCHAR	Ф. И. О. курьера
settlement_year	SMALLINT	Год отчёта
settlement_month	SMALLINT	месяц отчёта, где 1 — январь и 12 — декабрь.
orders_count	INT	количество заказов за период (месяц).
orders_total_sum	NUMERIC(14, 2)	общая стоимость заказов.
rate_avg	NUMERIC(4, 2)	средний рейтинг курьера по оценкам пользователей.
order_processing_fee	NUMERIC(14, 2)	сумма, удержанная компанией за обработку заказов,
courier_order_sum	NUMERIC(14, 2)	сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы
courier_tips_sum	NUMERIC(14, 2)	сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum	NUMERIC(14, 2)	сумма, которую необходимо перечислить курьеру.

Список таблиц в слое DDS


dds.dm_couriers	Справочник курьеров	courier_id, courier_name
dds.dm_orders Справочник заказов	order_id, sum (стоимость заказа)
dds.fct_deliveries	Факты доставок	delivery_ts (для группировки по месяцу), rate (рейтинг), tip_sum (чаевые)
dds.dm_timestamps	Справочник дат id,ts,year,month,day,time,date 

Сущности для загрузки из API

1. /couriers -> stg.couriers
_id	Уникальный идентификатор курьера
name Ф. И. О. курьера
2./deliveries -> stg.deliveries
order_id Идентификатор заказа
courier_id	Курьер доставки
delivery_ts	Период расчета (год/месяц)
rate	рейтинг курьера
tip_sum	суммы чаевых

Метод /restaurants из API для данной витрины не требуется.
