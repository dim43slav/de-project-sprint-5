create table cdm.dm_settlement_report(
	id serial
	,restaurant_id varchar NOT NULL
	,restaurant_name varchar NOT NULL
	,settlement_date date NOT NULL
	,orders_count integer NOT NULL
	,orders_total_sum numeric(14, 2) NOT NULL
	,orders_bonus_payment_sum numeric(14, 2) NOT NULL
	,orders_bonus_granted_sum numeric(14, 2) NOT NULL
	,order_processing_fee numeric(14, 2) NOT NULL
	,restaurant_reward_sum numeric(14, 2) NOT NULL
);

---------stg---------
 
stg.bonussystem_users;
stg.bonussystem_ranks
stg.bonussystem_events

DROP TABLE if exists stg.bonussystem_events;

CREATE TABLE stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);

DROP TABLE if exists stg.bonussystem_ranks;

CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0
);


DROP TABLE if exists stg.bonussystem_users;

CREATE TABLE stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL
);



drop table if exists stg.ordersystem_users;
create table stg.ordersystem_users(
	id serial not null
	,object_id varchar not null
	,object_value text not null
	,update_ts timestamp not null
);

alter table stg.ordersystem_users drop constraint if exists ordersystem_users_object_id_uindex;
alter table stg.ordersystem_users add constraint ordersystem_users_object_id_uindex UNIQUE(object_id);

drop table if exists stg.ordersystem_orders;
create table stg.ordersystem_orders(
	id serial not null
	,object_id varchar not null
	,object_value text not null
	,update_ts timestamp not null
);

alter table stg.ordersystem_orders drop constraint if exists ordersystem_orders_object_id_uindex;
alter table stg.ordersystem_orders add constraint ordersystem_orders_object_id_uindex UNIQUE(object_id);

drop table if exists stg.ordersystem_restaurants;
create table stg.ordersystem_restaurants(
	id serial not null
	,object_id varchar not null
	,object_value text not null
	,update_ts timestamp not null
);

alter table stg.ordersystem_restaurants drop constraint if exists ordersystem_restaurants_object_id_uindex;
alter table stg.ordersystem_restaurants add constraint ordersystem_restaurants_object_id_uindex UNIQUE(object_id);





CREATE TABLE clients
(
    client_id INTEGER NOT NULL
        CONSTRAINT clients_pk PRIMARY KEY,
    name      TEXT    NOT NULL,
    login     TEXT    NOT NULL
);

CREATE TABLE products
(
    product_id INTEGER        NOT NULL
        CONSTRAINT products_pk PRIMARY KEY,
    name       TEXT           NOT NULL,
    price      NUMERIC(14, 2) NOT NULL
);

CREATE TABLE sales
(
    client_id  INTEGER        NOT NULL
        CONSTRAINT sales_clients_client_id_fk REFERENCES clients,
    product_id INTEGER        NOT NULL
        CONSTRAINT sales_products_product_id_fk REFERENCES products,
    amount     INTEGER        NOT NULL,
    total_sum  NUMERIC(14, 2) NOT NULL,
    CONSTRAINT sales_pk PRIMARY KEY (client_id, product_id)
); 


drop table if exists dds.dm_users;
create table dds.dm_users(
id serial constraint dm_users_pkey primary key
,user_id int not null 
,user_name varchar not null
,user_login varchar not null);



drop table if exists dds.dm_restaurants;
create table dds.dm_restaurants(
	id serial constraint dm_restaurants_pkey primary key
	,restaurant_id int not null
	,restaurant_name varchar not null
	,active_from timestamp not null
	,active_to timestamp not null);
	

drop table if exists dds.dm_products;

create table dds.dm_products(
	id serial constraint dm_products_pkey primary key
	,product_id character varying not null
	,product_name character varying not null
	,product_price numeric(14,2) default 0 not null constraint dm_products_product_price_check check (product_price >=0)
	,restaurant_id int not null
	,active_from timestamp not null
	,active_to timestamp not null
);

alter table dds.dm_products add constraint dm_products_restaurant_id_fk foreign key (restaurant_id) references dds.dm_restaurants(id);


drop table if exists dds.dm_timestamps;

create table dds.dm_timestamps(
	id serial constraint dm_timestamps_pk primary key
	,ts timestamp
	,"year" smallint not null constraint dm_timestamps_year_check check (year >= 2022 and year <2500)
	,"month" smallint not null constraint dm_timestamps_month_check check (month >= 1 and month <=12)
	,"day" smallint not null constraint dm_timestamps_day_check check (day >= 1 and day <=31)
	,"time" time not null
	,"date" date not null
);

            
            
drop table if exists dds.dm_orders

create table dds.dm_orders(
id serial constraint dm_orders_pkey primary key,
order_key varchar not null,
order_status varchar not null,
user_id int not null,
restaurant_id int not null,
timestamp_id integer not null);

alter table dds.dm_orders add constraint dm_orders_user_id_fk foreign key (user_id) references dds.dm_users(id);
alter table dds.dm_orders add constraint dm_orders_restaurant_id_fk foreign key (restaurant_id) references dds.dm_restaurants(id);
alter table dds.dm_orders add constraint dm_orders_timestamp_id_fk foreign key (timestamp_id) references dds.dm_timestamps(id);


drop table if exists dds.fct_product_sales;

create table dds.fct_product_sales(
	id serial constraint fct_product_sales_pk primary key
	,product_id int not null
	,order_id int not null
	,count int default 0 not null constraint fct_product_sales_count_check check (count>=0)
	,price numeric(14,2) default 0 not null constraint fct_product_sales_price_check check (price>=0)
	,total_sum numeric(14,2) default 0 not null constraint fct_product_sales_total_sum_check check (total_sum>=0)
	,bonus_payment numeric(14,2) default 0 not null constraint fct_product_sales_bonus_payment_check check (bonus_payment>=0)
	,bonus_grant numeric(14,2) default 0 not null constraint fct_product_sales_bonus_grant_check check (bonus_grant>=0)
);
	
alter table dds.fct_product_sales add constraint fct_product_sales_product_id_fk foreign key (product_id) references dds.dm_products(id);
alter table dds.fct_product_sales add constraint fct_product_sales_order_id_fk foreign key (order_id) references dds.dm_orders(id);

alter table dds.dm_restaurants

ALTER TABLE dds.dm_users ALTER COLUMN user_id TYPE varchar;


select * from cdm.dm_settlement_report;


COPY dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to) FROM '/data/dm_restaurants_';
COPY dds.dm_products (id, restaurant_id, product_id, product_name, product_price, active_from, active_to) FROM '/data/dm_products_';
COPY dds.dm_timestamps (id, ts, year, month, day, "time", date) FROM '/data/dm_timestamps_';
COPY dds.dm_users (id, user_id, user_name, user_login) FROM '/data/dm_users_';
COPY dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id) FROM '/data/dm_orders_';
COPY dds.fct_product_sales (id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) FROM '/data/fct_product_sales_'; 


select count(*) from dds.dm_orders where order_status = 'CLOSED';

insert into cdm.dm_settlement_report(
	restaurant_id
	,restaurant_name
	,settlement_date
	,orders_count
	,orders_total_sum
	,orders_bonus_payment_sum
	,orders_bonus_granted_sum
	,order_processing_fee
	,restaurant_reward_sum)
with pre_agg as(	
select
	d.restaurant_id
	,r.restaurant_name 
	,t.date as settlement_date
	,f.count 
	,d.id
	,f.price
	,f.total_sum
	,f.bonus_payment 
	,f.bonus_grant 
from dds.fct_product_sales f 
left join dds.dm_orders d on d.id  = f.order_id 
left join dds.dm_restaurants r on r.id = d.restaurant_id
left join dds.dm_timestamps t on t.id  = d.timestamp_id 
where order_status = 'CLOSED')
select
	restaurant_id
	,restaurant_name 
	,settlement_date
	,count(distinct id) as orders_count
	,sum(total_sum) as orders_total_sum
	,sum(bonus_payment) as orders_bonus_payment_sum
	,sum(bonus_grant) as orders_bonus_granted_sum
	,sum(total_sum)*0.25 as order_processing_fee
	,sum(total_sum) - sum(total_sum)*0.25 - sum(bonus_payment) as restaurant_reward_sum
from pre_agg
group by 
	restaurant_id
	,restaurant_name 
	,settlement_date
ON CONFLICT (id)
do update set 
	restaurant_id = EXCLUDED.restaurant_id,
	settlement_date = EXCLUDED.settlement_date,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
	orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
	order_processing_fee = EXCLUDED.order_processing_fee,
	restaurant_reward_sum = EXCLUDED.restaurant_reward_sum

	
	
	
	
drop table if exists public.outbox;
create table public.outbox(
	id int not null
	,object_id int not null
	,record_ts timestamp not null
	,type varchar not null
	,payload text not null);
	
select * from stg.bonussystem_ranks;

alter table stg.bonussystem_ranks add constraint bonussystem_ranks_pkey primary key (id);



select * from stg.bonussystem_users;
alter table stg.bonussystem_users add constraint bonussystem_users_pkey primary key(id);


select 
	workflow_settings->>'last_loaded_id' as last_id
from stg.srv_wf_settings
where workflow_key = 'example_events_origin_to_stg_workflow';

select * from stg.bonussystem_events;


alter table stg.bonussystem_events add constraint bonussystem_events_pkey primary key (id);



select * from stg.ordersystem_users;

select * from stg.ordersystem_orders;

drop table if exists dds.srv_wf_settings;
create table dds.srv_wf_settings(
	id int not null primary key
	,workflow_key varchar not null
	,workflow_settings json not null);
	

--select * from dds.srv_wf_settings;


DROP TABLE dds.srv_wf_settings;

CREATE TABLE dds.srv_wf_settings (
	id serial NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);


select * from dds.dm_users;


В STG данные о пользователях лежат в формате JSON. Поэтому логика шага может выглядеть таким образом:

Прочитать данные из таблицы в STG: выгрузить в Python.

С помощью Python преобразовать JSON к объекту. Воспользуйтесь функцией str2json из файла lib/dict_util.py.

Вставить объекты в таблицу dds.dm_users.



select * from stg.bonussystem_users;

select
	id
	,object_value::json->>'_id' as user_id
	,object_value::json->>'name' as user_name
	,object_value::json->>'login' as user_login
from stg.ordersystem_users;

select * from stg.ordersystem_users;

id,user_id,user_name,user_login




DROP table if exists dds.dm_users;

CREATE TABLE dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id)
);

alter table dds.srv_wf_settings add CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id);

alter table dds.srv_wf_settings add CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key);


select * from dds.srv_wf_settings;



select * from stg.ordersystem_restaurants or2;
select * from dds.dm_restaurants dr;

id,restaurant_id,restaurant_name,active_from,active_to


select
	id,
	object_value::json->>'_id' as restaurant_id,
	object_value::json->>'name' as restaurant_name,
	object_value::json->>'update_ts' as active_from,
	object_value::json->>'_id' as active_to
from stg.ordersystem_restaurants or2;


id,ts,year,month,day,time,date

select * from dds.dm_timestamps;
select * from stg.ordersystem_orders oo
where object_value::json ->>'final_status' in('CLOSED','CANCELLED');

select 
	object_value::json->>'date' as ts
	,object_value
	,extract(year from cast(object_value::json->>'date' as date)) as year
	,extract(month from cast(object_value::json->>'date' as date)) as month
	,extract(day from cast(object_value::json->>'date' as date)) as day
	,cast(object_value::json->>'date' as time)::text as time
	,cast(object_value::json->>'date' as date)::text as date
from stg.ordersystem_orders oo
where object_id = '68a4ab6a6dc355956fadce48';

{"_id": "68a4ab6a6dc355956fadce48",
"bonus_grant": 304,
"bonus_payment": 385,
"cost": 3420,
"date": "2025-08-19 16:50:49",
"final_status": "CLOSED",
"order_items": [{"id": "6276e8cd0cf48b4cded00872",
"name": "СЭНДВИЧ С ВЕТЧИНОЙ И СЫРОМ",
"price": 120,
"quantity": 5 },
{"id": "6276e8cd0cf48b4cded0086e",
"name": "РОЛЛ С ВЕТЧИНОЙ И ОМЛЕТОМ",
"price": 120,
"quantity": 1 },
{"id": "6276e8cd0cf48b4cded00878",
"name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
"price": 180,
"quantity": 3 },
{"id": "6276e8cd0cf48b4cded00877",
"name": "КИШ С ВЕТЧИНОЙ И ГРИБАМИ",
"price": 120,
"quantity": 5 },
{"id": "6276e8cd0cf48b4cded0086c",
"name": "ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ",
"price": 60,
"quantity": 4 },
{"id": "6276e8cd0cf48b4cded0087a",
"name": "НОРВЕЖСКИЙ СЭНДВИЧ С РЫБОЙ И ЯЙЦОМ",
"price": 180,
"quantity": 4 },
{"id": "6276e8cd0cf48b4cded00874",
"name": "ПИЦЦЕТТА С КУРИЦЕЙ",
"price": 120,
"quantity": 5 }],
"payment": 3420,
"restaurant": {"id": "626a81cfefa404208fe9abae" },
"statuses": [{"dttm": "2025-08-19 16:50:49",
"status": "CLOSED" },
{"dttm": "2025-08-19 16:04:03",
"status": "DELIVERING" },
{"dttm": "2025-08-19 15:08:45",
"status": "COOKING" },
{"dttm": "2025-08-19 14:35:33",
"status": "OPEN" }],
"update_ts": "2025-08-19 16:50:50",
"user": {"id": "626a81ce9a8cd1920641e279" }}

select * from stg.ordersystem_restaurants or2;
select * from dds.dm_products dp;

id,product_id,product_name,product_price,restaurant_id,active_from,active_to


select
	id,
	jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'_id' as product_id
	,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'name' as product_name
	,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'price' as product_price
	,jsonb_array_elements((object_value::json->>'menu')::jsonb)->>'name' as product_name
	,id as restaurant_id
	,object_id
	,update_ts as active_from
	,'2099-12-31 00:00:00.000'::timestamp as active_to
from
	stg.ordersystem_restaurants or2;


truncate table dds.srv_wf_settings cascade;



В STG данные о заказе лежат в формате JSON. Поэтому логика шага может выглядеть таким образом:

Прочитать данные из таблицы в STG.
Преобразовать JSON к объекту.

Найти версию ресторана по id и update_ts.
Найти версию пользователя по id и update_ts.
Найти временную метку по date в заказе.
Вставить объект заказа в таблицу.

select * from dds.dm_orders;

select * from stg.ordersystem_orders

select 
	timestamp_id
from(

select
    o.id as id1
    ,o.object_id as order_key
    ,o.object_value::json->>'final_status' as order_status
    ,o.object_value::json->>'update_ts'
--    ,dr.id as restaurant_id
--    ,dt.id as timestamp_id
--    ,ur.id as user_id
from
    stg.ordersystem_orders o
join dds.dm_restaurants dr
    on
    dr.restaurant_id = o.object_value::json->'restaurant'->>'id'
join dds.dm_users ur
    on
    ur.user_id = o.object_value::json->'user'->>'id'
--join dds.dm_timestamps dt 
--	on dt.ts::timestamp = (o.object_value::json->>'update_ts')::timestamp
where o.object_id = '68a4ab6a6dc355956fadce48'
	
) a
where id1 not in (select id from dds.dm_timestamps)


select * from dds.dm_timestamps where ts > '2025-08-19 16:50:50';

select
    o.id as id1
    ,o.object_id as order_key
    ,o.object_value::json->>'final_status' as order_status
    ,dr.id as restaurant_id
    ,o.id as timestamp_id
    ,ur.id as user_id
from
    stg.ordersystem_orders o
join dds.dm_restaurants dr
    on
    dr.restaurant_id = o.object_value::json->'restaurant'->>'id'
join dds.dm_users ur
    on
    ur.user_id = o.object_value::json->>'update_ts'



select * from stg.ordersystem_orders where id = 9330;
	
id,order_key,order_status,user_id,restaurant_id,timestamp_id
	
join dds.dm_restaurants dr on dr.restaurant_id = o.object_value::json->'restaurant'->>'id' and dr.active_to = '2099-12-31 00:00:00.000'::timestamp
join dds.dm_timestamps dt on dt.ts::timestamp = (o.object_value::json->>'update_ts')::timestamp
join dds.dm_users du on du.user_id =  o.object_value::json->'user'->>'id'

{"_id": "687cb063a0ac572440d75432",
"bonus_grant": 30,
"bonus_payment": 306,
"cost": 900,
"date": "2025-07-20 09:01:23",
"final_status": "CLOSED",
"order_items": [{"id": "845d42f8af7705492d2d576a",
"name": "Хинкали с креветкой Том Ям 3 шт",
"price": 450,
"quantity": 2 }],
"payment": 900,
"restaurant": {"id": "a51e4e31ae4602047ec52534" },
"statuses": [{"dttm": "2025-07-20 09:01:23",
"status": "CLOSED" },
{"dttm": "2025-07-20 08:42:02",
"status": "DELIVERING" },
{"dttm": "2025-07-20 08:35:59",
"status": "COOKING" },
{"dttm": "2025-07-20 08:03:07",
"status": "OPEN" }],
"update_ts": "2025-07-20 09:01:23",
"user": {"id": "626a81ce9a8cd1920641e29c" }}



select
    o.id
    ,o.object_id as order_key
    ,o.object_value::json->>'final_status' as order_status
    ,dr.id as restaurant_id
    ,o.id as timestamp_id
    ,ur.id as user_id
from
    stg.ordersystem_orders o
join dds.dm_restaurants dr
    on
    dr.restaurant_id = o.object_value::json->'restaurant'->>'id'
join dds.dm_users ur
    on
    ur.user_id = o.object_value::json->'user'->>'id'

    
drop table if exists stg.bonussystem_ranks;
    
CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0)
);


drop table if exists stg.bonussystem_users;

CREATE TABLE if not exists stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);


DROP TABLE stg.bonussystem_events;

CREATE TABLE stg.bonussystem_events (
	id int4 NOT null primary KEY,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);


DROP TABLE if exists dds.dm_restaurants cascade;
DROP TABLE if exists dds.dm_timestamps cascade;
DROP TABLE if exists dds.dm_users cascade;
DROP TABLE if exists dds.srv_wf_settings cascade;
DROP TABLE if exists dds.dm_orders cascade;
DROP TABLE if exists dds.dm_products cascade;
DROP TABLE if exists dds.fct_product_sales cascade;
drop table if exists dds.fct_deliveries cascade;
drop table if exists dds.dm_couriers cascade;
-- dds.dm_restaurants definition

-- Drop table



drop table if exists dds.dm_couriers;
CREATE TABLE dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL
);


-- DROP TABLE dds.dm_restaurants cascade;

CREATE TABLE dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);


-- dds.dm_timestamps definition

-- Drop table

-- DROP TABLE dds.dm_timestamps cascade;

CREATE TABLE dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pk PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);


-- dds.dm_users definition

-- Drop table

-- DROP TABLE dds.dm_users cascade;

CREATE TABLE dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id)
);


-- dds.srv_wf_settings definition

-- Drop table

-- DROP TABLE dds.srv_wf_settings cascade;

CREATE TABLE dds.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);


-- dds.dm_orders definition

-- Drop table

-- DROP TABLE dds.dm_orders cascade;

CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	courier_id int4,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
);
-- dds.dm_orders внешние включи

ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_timestamp_id_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_user_id_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);


-- dds.dm_products definition

-- Drop table

-- DROP TABLE dds.dm_products cascade;

CREATE TABLE dds.dm_products (
	id serial4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	restaurant_id int4 NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
	CONSTRAINT dm_products_product_price_check CHECK ((product_price >= (0)::numeric)),
	CONSTRAINT dm_products_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);


-- dds.fct_product_sales definition

-- Drop table

-- DROP TABLE dds.fct_product_sales cascade;

CREATE TABLE dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pk PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric)),
	CONSTRAINT fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id)
);


id,order_id,courier_id,delivery_ts,address,rate,tip_sum,total_sum


drop table if exists dds.fct_deliveries;
CREATE TABLE dds.fct_deliveries (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES dds.dm_orders (id),
    courier_id INT REFERENCES dds.dm_couriers (id),
    delivery_ts TIMESTAMP,
    address VARCHAR ,
    rate SMALLINT default 0 CHECK (rate >= 1 AND rate <= 5),
    tip_sum NUMERIC(14, 2) DEFAULT 0 CHECK (tip_sum >= 0),
    total_sum NUMERIC(14,2) not null default 0 check (total_sum >=0)
);



insert into dds.dm_orders 

select
                        o.id as id
                        ,o.object_id as order_key
                        ,o.object_value::json->>'final_status' as order_status
                        ,dr.id as restaurant_id
                        ,dt.id as timestamp_id
                        ,ur.id as user_id
                        ,c.id as courier_id
                    from
                        stg.ordersystem_orders o
                    join dds.dm_restaurants dr
                        on
                        dr.restaurant_id = o.object_value::json->'restaurant'->>'id'
                    join dds.dm_users ur
                        on
                        ur.user_id = o.object_value::json->'user'->>'id'
                    join dds.dm_timestamps dt
                        on dt.ts::timestamp = (o.object_value::json->>'date')::timestamp
                    left join stg.deliveries sd 
                        on 
                        sd.order_id = o.object_id
                    left join stg.couriers c on c.object_id = sd.courier_id

              select * from stg.deliveries d ;          
                        
                 
              
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
        ,delivery_ts
        ,address
        ,rate
        ,tip_sum
        ,total_sum
    from pre_agg
    
    
    
select * from stg.deliveries;
select * from dds.dm_orders do2;
    

{"_id": "68958497a176115892c5131e",
"bonus_grant": 452,
"bonus_payment": 15,
"cost": 4540,
"date": "2025-08-08 05:01:11",
"final_status": "CLOSED",
"order_items": [{"id": "e0f74cc69658b5b3fd721a6e",
"name": "Казан кебаб с бараниной",
"price": 520,
"quantity": 2 },
{"id": "bfdf489791e9bc1eb3eefbf5",
"name": "Лагман",
"price": 350,
"quantity": 10 }],
"payment": 4540,
"restaurant": {"id": "ebfa4c9b8dadfc1da37ab58d" },
"statuses": [{"dttm": "2025-08-08 05:01:11",
"status": "CLOSED" },
{"dttm": "2025-08-08 04:03:55",
"status": "DELIVERING" },
{"dttm": "2025-08-08 03:17:35",
"status": "COOKING" },
{"dttm": "2025-08-08 02:43:08",
"status": "OPEN" }],
"update_ts": "2025-08-08 05:01:11",
"user": {"id": "626a81ce9a8cd1920641e261" }}


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



id,product_id,order_id,count,price,total_sum,bonus_payment,bonus_grant


 68a7b20a21afd6611008931e  e0f74cc69658b5b3fd721a6e   1560.00      3  520.00         63.00      535.00

select * from stg.ordersystem_orders oo where object_id = '689e765bc4b752bda9a91e61';--'68a7b20a21afd6611008931e';


{"_id": "68a7b20a21afd6611008931e",
"bonus_grant": 535,
"bonus_payment": 63,
"cost": 5410,
"date": "2025-08-21 23:55:54",
"final_status": "CLOSED",
"order_items": [{"id": "8d5c4d1598e06f880986abe4",
"name": "Шурпа из баранины",
"price": 350,
"quantity": 3 },
{"id": "139b406ba16bda8ba646d7b1",
"name": "Шурпа из говядины",
"price": 350,
"quantity": 2 },
{"id": "b92d4d66bc844b7fd7ade634",
"name": "Плов чайханский",
"price": 420,
"quantity": 2 },
{"id": "30ec47e5baea70b664df6bff",
"name": "Плов ферганский",
"price": 420,
"quantity": 3 },
{"id": "e0f74cc69658b5b3fd721a6e",
"name": "Казан кебаб с бараниной",
"price": 520,
"quantity": 3 }],
"payment": 5410,
"restaurant": {"id": "ebfa4c9b8dadfc1da37ab58d" },
"statuses": [{"dttm": "2025-08-21 23:55:54",
"status": "CLOSED" },
{"dttm": "2025-08-21 23:19:12",
"status": "DELIVERING" },
{"dttm": "2025-08-21 22:50:44",
"status": "COOKING" },
{"dttm": "2025-08-21 22:47:42",
"status": "OPEN" }],
"update_ts": "2025-08-21 23:55:54",
"user": {"id": "626a81ce9a8cd1920641e27a" }}

 {"user_id": 15,
"order_id": "687c98df3d30ffc531936a08",
"order_date": "2025-07-20 07:21:03",
"product_payments": [{"product_id": "6276e8cd0cf48b4cded00879",
"product_name": "\u0420\u041e\u041b\u041b \u0421 \u0420\u042b\u0411\u041e\u0419 \u0418 \u0421\u041e\u0423\u0421\u041e\u041c \u0422\u0410\u0420\u0422\u0410\u0420",
"price": 180,
"quantity": 2,
"product_cost": 360,
"bonus_payment": 360,
"bonus_grant": 0 },
{"product_id": "6276e8cd0cf48b4cded0087a",
"product_name": "\u041d\u041e\u0420\u0412\u0415\u0416\u0421\u041a\u0418\u0419 \u0421\u042d\u041d\u0414\u0412\u0418\u0427 \u0421 \u0420\u042b\u0411\u041e\u0419 \u0418 \u042f\u0419\u0426\u041e\u041c",
"price": 180,
"quantity": 3,
"product_cost": 540,
"bonus_payment": 159.0,
"bonus_grant": 38 },
{"product_id": "6276e8cd0cf48b4cded0086f",
"product_name": "\u0420\u041e\u041b\u041b \u0421 \u041a\u0423\u0420\u0418\u0426\u0415\u0419 \u0418 \u041f\u0415\u0421\u0422\u041e",
"price": 120,
"quantity": 2,
"product_cost": 240,
"bonus_payment": 0.0,
"bonus_grant": 24 }]}
 
select distinct
    o.order_key AS order_key,
    dp.product_id AS product_id,
    pr.total_sum,
    pr.count,
    pr.price,
    pr.bonus_payment,
    pr.bonus_grant
FROM dds.fct_product_sales AS pr
    INNER JOIN dds.dm_orders AS o
        ON pr.order_id = o.id
    INNER JOIN dds.dm_timestamps AS t
        ON o.timestamp_id = t.id
    INNER JOIN dds.dm_products AS dp
        ON pr.product_id = dp.id
WHERE ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1
and order_key = '689e765bc4b752bda9a91e61'

ORDER BY ts DESC
;






select
	dr.restaurant_id as restaurant_id
	,dr.restaurant_name as restaurant_name
	,count(order_id) as orders_count
	,now()::Date as settlement_date
	,sum(total_sum) as orders_total_sum
	,sum(bonus_payment) as orders_bonus_payment_sum
	,sum(bonus_grant) as orders_bonus_granted_sum
	,sum(total_sum)*0.25 as order_processing_fee
	,sum(total_sum) - sum(bonus_payment) - sum(total_sum)*0.25 as restaurant_reward_sum
from dds.fct_product_sales f
join dds.dm_orders do2 on do2.id = f.order_id
join dds.dm_restaurants dr on dr.id = do2.restaurant_id
where do2.order_status = 'CLOSED';





truncate table cdm.dm_settlement_report;
insert into cdm.dm_settlement_report(
	restaurant_id
	,restaurant_name
	,settlement_date
	,orders_count
	,orders_total_sum
	,orders_bonus_payment_sum
	,orders_bonus_granted_sum
	,order_processing_fee
	,restaurant_reward_sum)
with pre_agg as(	
select
	d.restaurant_id
	,r.restaurant_name 
	,t.date as settlement_date
	,f.count 
	,d.id
	,f.price
	,f.total_sum
	,f.bonus_payment 
	,f.bonus_grant 
from dds.fct_product_sales f 
left join dds.dm_orders d on d.id  = f.order_id 
left join dds.dm_restaurants r on r.id = d.restaurant_id
left join dds.dm_timestamps t on t.id  = d.timestamp_id 
where order_status = 'CLOSED')
select
	restaurant_id
	,restaurant_name 
	,settlement_date
	,count(distinct id) as orders_count
	,sum(total_sum) as orders_total_sum
	,sum(bonus_payment) as orders_bonus_payment_sum
	,sum(bonus_grant) as orders_bonus_granted_sum
	,sum(total_sum)*0.25 as order_processing_fee
	,sum(total_sum) - sum(total_sum)*0.25 - sum(bonus_payment) as restaurant_reward_sum
from pre_agg
group by 
	restaurant_id
	,restaurant_name 
	,settlement_date
ON CONFLICT (id)
do update set 
	restaurant_id = EXCLUDED.restaurant_id,
	settlement_date = EXCLUDED.settlement_date,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
	orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
	order_processing_fee = EXCLUDED.order_processing_fee,
	restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
	
	
	
	
SELECT
	restaurant_name,
	settlement_date,
	orders_count,
	orders_total_sum,
	orders_bonus_payment_sum,
	orders_bonus_granted_sum,
	order_processing_fee,
	restaurant_reward_sum
FROM cdm.dm_settlement_report
where
(settlement_date::date >= (now()at time zone 'utc')::date - 3
AND
settlement_date::date <= (now()at time zone 'utc')::date - 1)
ORDER BY settlement_date desc;


id,test_date_time,test_name,test_result

insert
	into
	public_test.testing_result (test_date_time,
	test_name,
	test_result)
	
select
	now()::timestamp as test_date_time,
	'test_01' as test_name,
	case
		when count(*) = 0 then true
		else false
	end as test_result
from
	public_test.dm_settlement_report_actual a
full join public_test.dm_settlement_report_expected e on
	e.id = a.id
	and e.restaurant_id = a.restaurant_id
	and e.restaurant_name = a.restaurant_name
	and e.settlement_year = a.settlement_year
	and e.settlement_month = a.settlement_month
	and e.orders_count = a.orders_count
	and e.orders_total_sum = a.orders_total_sum
	and e.orders_bonus_payment_sum = a.orders_bonus_payment_sum
	and e.orders_bonus_granted_sum = a.orders_bonus_granted_sum
	and e.order_processing_fee = a.order_processing_fee
	and e.restaurant_reward_sum = a.restaurant_reward_sum
where
	e.id is null


	
	
	
	

create table cdm.courier_payment(
	id serial not null
	,courier_id varchar not null
	,courier_name varchar not null
	,settlement_year int
	,settlement_month smallint not null
	,orders_count int not null
	,orders_total_sum numeric not null
	,rate_avg numeric not null
	,order_processing_fee numeric not null
	,courier_order_sum numeric not null
	,courier_tips_sum numeric not null
	,courier_reward_sum numeric not null);




{'order_id': '68b334202e3f34a6f9440657',
'order_ts': '2025-08-30 17:25:52.090000',
'delivery_id': 'n5mn1l1ji0dtl2jefy147os',
'courier_id': 'sq8lgjx76dv1e1ah60mw29g',
'address': 'Ул. Старая, 13, кв. 377',
'delivery_ts': '2025-08-30 19:12:54.474000',
'rate': 5,
'sum': 5800,
'tip_sum': 580 }


create table stg.deliveries(
order_id
,order_ts
,delivery_id
,courier_id
,address
,delivery_ts
,rate
,sum
,tip_sum)


drop table if exists stg.deliveries;
create table stg.deliveries(
id serial not null primary key,
object_id varchar,
object_value text,
update_ts timestamp);

drop table if exists stg.restaurants;
create table stg.restaurants(
id serial not null primary key,
object_id varchar,
object_value text,
update_ts timestamp);

drop table if exists stg.couriers;
create table stg.couriers(
id serial not null primary key,
object_id varchar,
object_value text,
update_ts timestamp);




delete from stg.srv_wf_settings where workflow_key in ('project_restaurants_origin_to_stg','project_couriers_origin_to_stg');



drop table if exists stg.deliveries;
create table stg.deliveries(
	id serial primary key
	,order_id varchar(40)
	,order_ts timestamp
	,delivery_id varchar(40)
	,courier_id varchar(40)
	,address varchar
	,delivery_ts timestamp
	,rate smallint
	,tip_sum numeric(12,2)
	,sum numeric(12,2)
);

delete from stg.srv_wf_settings where workflow_key = 'project_deliveries_origin_to_stg'

select * from stg.deliveries d ;

order_ts,delivery_id,courier_id,address,delivery_ts,rate,tip_sum,sum

select * from stg.couriers;





ALTER TABLE dds.dm_orders
ADD COLUMN courier_id INT REFERENCES dds.dm_couriers (id);





CREATE TABLE cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year SMALLINT NOT NULL
        CHECK (settlement_year >= 2022 AND settlement_year < 2500),
    settlement_month SMALLINT NOT NULL
        CHECK (settlement_month >= 1 AND settlement_month <= 12),
    orders_count INT NOT NULL
        CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0
        CHECK (orders_total_sum >= 0),
    rate_avg NUMERIC(4, 2) NOT NULL DEFAULT 0
        CHECK (rate_avg >= 0 AND rate_avg <= 5),
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0
        CHECK (order_processing_fee >= 0),
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0
        CHECK (courier_order_sum >= 0),
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0
        CHECK (courier_tips_sum >= 0),
    courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0
        CHECK (courier_reward_sum >= 0),
    UNIQUE (courier_id, settlement_year, settlement_month)
);


select * from dds.dm_orders where courier_id is not null;



with month_agg as(
select
	dc.id
	,dc.name as courier_name
	,dt."year"
	,dt.month
	,count(f.order_id) as orders_count
	,sum(f.total_sum) as orders_total_sum
	,avg(f.rate) as rate_avg
	,sum(f.total_sum) * 0.25 as order_processing_fee
	,null as courier_order_sum
	,sum(f.tip_sum) as courier_tips_sum
	,null as courier_reward_sum
from dds.fct_deliveries f
join dds.dm_orders o on o.id = f.order_id
join dds.dm_couriers dc on dc.id = o.courier_id
join dds.dm_timestamps dt on dt.id = o.timestamp_id
group by 
	dc.id
	,dc.name
	,dt."year"
	,dt.month),
coeff as(
	select
		id
		,courier_name
		,"year"
		,month
		,orders_count
		,orders_total_sum
		,rate_avg
		,order_processing_fee
		,case when rate_avg < 4 then 0.05
		when rate_avg >= 4 and rate_avg < 4.5 then 0.07
		when rate_avg < 4.9 and rate_avg >= 4.5 then 0.08
		when rate_avg >= 4.9 then 0.1 end as coeff
		,null as courier_order_sum
		,courier_tips_sum
		,null as courier_reward_sum
	from month_agg)
		select
			id
			,courier_name
			,"year"
			,month
			,orders_count
			,orders_total_sum
			,rate_avg
			,order_processing_fee
			,orders_total_sum * coeff as courier_order_sum
			,courier_tips_sum
			,case when coeff = 0.05 then greatest(orders_total_sum * coeff + courier_tips_sum*0.95,100) 
				when coeff = 0.07 then greatest(orders_total_sum * coeff + courier_tips_sum*0.95,150)  
				when coeff = 0.08 then greatest(orders_total_sum * coeff + courier_tips_sum*0.95,175) 
				when coeff = 0.1 then greatest(orders_total_sum * coeff + courier_tips_sum*0.95,200)  
			end as courier_reward_sum
		from coeff
	

r < 4 — 5% от заказа, но не менее 100 р.;
4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
4.9 <= r — 10% от заказа, но не менее 200 р.



id

courier_id
courier_name
settlement_year
settlement_month
orders_count
orders_total_sum
rate_avg
order_processing_fee
courier_order_sum
courier_tips_sum
courier_reward_sum

