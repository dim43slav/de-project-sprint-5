class DmLoader:
    def load_dm_couriers(self,dwh_connector):
        conn = dwh_connector.get_conn()
        cursor = conn.cursor()
        cursor.execute(
        """
            TRUNCATE TABLE cdm.dm_courier_ledger;
        """)

        cursor.execute(
        """
            INSERT INTO cdm.dm_courier_ledger(courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
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
                                id as courier_id
                                ,courier_name
                                ,"year" as settlement_year
                                ,month as settlement_month
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
                            from coeff;
        """)
        conn.commit()
        cursor.close()
        conn.close()