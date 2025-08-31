import requests


class Loader:
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
    params = {'sort_field': 'id', 'sort_direction': 'asc'}
    headers = {
        'X-Nickname': 'dim43lsav',
        'X-Cohort': '1',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
    }


    def stg_loader(self,sort_field,sort_direction,limit,offset,method,step):
        headers = self.headers
        params = self.params
        url = self.url

        is_empty_response = 0
        while is_empty_response != 1:
            response = requests.get(f'{url}{method}?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}', params=params, headers=headers)
            print(response.json())
            offset = offset + step
            if response.text.strip() == '[]':
                is_empty_response = 1
        return 'method loaded'

    def get_restaurants(self):
        sort_field = 'id'
        sort_direction = 'asc'
        limit = 10
        offset = 0
        step = 10
        method = 'restaurants'

        message = self.stg_loader(self,sort_field,sort_direction,limit,offset,method,step)
        return message

    def get_couriers(self):
        sort_field = 'id'
        sort_direction = 'asc'
        limit = 10
        offset = 0
        step = 100
        method = 'couriers'

        message = self.stg_loader(self,sort_field,sort_direction,limit,offset,method,step)
        return message

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