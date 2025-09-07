import requests
from lib import PgConnect
from datetime import datetime
import json


class Loader:
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
    params = {'sort_field': 'id', 'sort_direction': 'asc'}
    headers = {
        'X-Nickname': 'dim43lsav',
        'X-Cohort': '1',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
    }
    ts = datetime.now()

    def get_last_id(self,workflow_key, cursor):
        cursor.execute(
        """
            SELECT workflow_settings FROM stg.srv_wf_settings
            where workflow_key = %s
        """,
        (workflow_key, )
        )

        workflow_setting = cursor.fetchall()
        return workflow_setting

    def set_last_id(self,workflow_key, cursor, tablename):
        cursor.execute(
        f"""
            select max(id) as id FROM stg.{tablename}
        """
        )

        current_setting = cursor.fetchall()[0][0]
        current_setting_json = {"last_loaded_id": current_setting}


        cursor.execute(
        """
            DELETE FROM stg.srv_wf_settings
            where workflow_key = %s
        """,
        (workflow_key, )
        )


        cursor.execute(
        """
            INSERT INTO stg.srv_wf_settings(workflow_key,workflow_settings)
            VALUES(%s,%s)
        """,
        (workflow_key,json.dumps(current_setting_json,ensure_ascii=False))
        )

        return current_setting

    def load_restaurants(self,data,cursor):
        for item in data:
                cursor.execute(
                    """
                        INSERT INTO stg.restaurants(object_id,object_value,update_ts)
                        VALUES (%s, %s, %s)
                    """,
                    (item['_id'],json.dumps(item,ensure_ascii=False),self.ts)
                )

    def load_couriers(self,data,cursor):
        for item in data:
                cursor.execute(
                    """
                        INSERT INTO stg.couriers(object_id,object_value,update_ts)
                        VALUES (%s, %s, %s)
                    """,
                    (item['_id'],json.dumps(item,ensure_ascii=False),self.ts)
                )

    def data_get(self,sort_field,sort_direction,limit,offset,method,step, dwh_dest, is_empty_response):
        headers = self.headers
        params = self.params
        url = self.url
        response = requests.get(f'{url}{method}?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}', params=params, headers=headers)

        return response, is_empty_response


    def get_restaurants(self, dwh_dest):
        sort_field = 'id'
        sort_direction = 'asc'
        limit = 10
        offset = 0
        step = 10
        is_empty_response = 0
        method = 'restaurants'
        tablename = 'restaurants'
        workflow_key = 'project_restaurants_origin_to_stg'

        conn = dwh_dest.get_conn()
        cursor = conn.cursor()

        wf_setting = self.get_last_id(workflow_key, cursor)
        if not wf_setting:
            pass
        else:
            offset = wf_setting[0][0]['last_loaded_id']
            print(f'offset = {offset}')

        #Получаем data из источника
        while is_empty_response != 1:
            response = self.data_get(sort_field,sort_direction,limit,offset,method,step,dwh_dest,is_empty_response)
            if response[0].text.strip() == '[]':
                break
            data = response[0].json()
            offset = offset + step

            #грузим data в бд
            self.load_restaurants(data, cursor)

            current_setting = self.set_last_id(workflow_key, cursor, tablename)
            print(f'loaded {current_setting} rows in stg.{tablename}')

        conn.commit()
        cursor.close()
        conn.close()

        return 'restaurants loaded'

    def get_couriers(self, dwh_dest):
        sort_field = 'id'
        sort_direction = 'asc'
        limit = 10
        offset = 0
        step = 10
        is_empty_response = 0
        method = 'couriers'
        tablename = 'couriers'
        workflow_key = 'project_couriers_origin_to_stg'

        conn = dwh_dest.get_conn()
        cursor = conn.cursor()

        wf_setting = self.get_last_id(workflow_key, cursor)
        if not wf_setting:
            pass
        else:
            offset = wf_setting[0][0]['last_loaded_id']
            print(f'offset = {offset}')

        #Получаем data из источника
        while is_empty_response != 1:
            response = self.data_get(sort_field,sort_direction,limit,offset,method,step,dwh_dest,is_empty_response)
            if response[0].text.strip() == '[]':
                break
            data = response[0].json()
            offset = offset + step

            #грузим data в бд
            self.load_restaurants(data, cursor)

            current_setting = self.set_last_id(workflow_key, cursor, tablename)
            print(f'loaded {current_setting} rows in stg.{tablename}')

        #грузим data в бд
        self.load_couriers(data, cursor)

        conn.commit()
        cursor.close()
        conn.close()

        return 'couriers loaded'

    def get_deliveries(self):
        sort_field = 'id'
        sort_direction = 'asc'
        limit = 10
        offset = 0
        step = 100
        method = 'deliveries'

        message = self.stg_loader(self,sort_field,sort_direction,limit,offset,method,step)
        return message


    #print(get_couriers())