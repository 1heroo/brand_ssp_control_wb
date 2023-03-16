import asyncio
import json

import aiohttp
import pandas as pd


class BaseUtils:

    @staticmethod
    async def make_get_request(url, headers, no_json=False):
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as session:
            async with session.get(url=url) as response:
                print(response.status)

                if response.status == 200:
                    return True if no_json else json.loads(await response.text())

    @staticmethod
    async def make_post_request(url, headers, payload, no_json=False):
        async with aiohttp.ClientSession(trust_env=True, headers=headers) as session:
            async with session.post(url=url, json=payload) as response:
                print(response.status)

                if response.status == 200:
                    return True if no_json else json.loads(await response.text())

    @staticmethod
    def find_max_ssp_column(df: pd.DataFrame, index):
        ssps = []

        for column in df.columns:
            if 'ssp' in column:
                ssps.append(
                    (column, int(df[column][index]))
                )

        return max(ssps, key=lambda item: item[-1])


class ParsingUtils(BaseUtils):

    async def get_products(self, brand_ids):
        products = await self.get_all_catalogs_from_brand(brand_ids=brand_ids)
        output_data = []

        tasks = []
        count = 1

        for product in products:
            task = asyncio.create_task(self.get_detail(nm=product.get('id')))
            tasks.append(task)
            count += 1

            if count % 50 == 0:
                print(count, 'product data')
                output_data += await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

        output_data += await asyncio.gather(*tasks, return_exceptions=True)
        output_data = [item for item in output_data if not isinstance(item, Exception) and item]
        return output_data

    async def get_detail(self, nm):
        card_url = make_head(int(nm)) + make_tail(str(nm), 'ru/card.json')
        obj = {}
        card = await self.make_get_request(url=card_url, headers={})

        detail_url = f'https://card.wb.ru/cards/detail?spp=27&regions=80,64,38,4,83,33,68,70,69,30,86,75,40,1,22,66,31,48,110,71&pricemarginCoeff=1.0&reg=1&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&sppFixGeo=4&dest=-455203&nm={nm}'
        detail = await self.make_get_request(detail_url, headers={})
        detail = detail['data']['products'] if detail else {}

        obj.update({
            'card': card if card else {},
            'detail': detail[0] if detail else {},
        })
        return obj

    async def get_all_catalogs_from_brand(self, brand_ids):
        products = []
        for brand_id in brand_ids:
            url = 'https://catalog.wb.ru/brands/h/catalog?appType=1&brand=%s&couponsGeo=12,3,18,15,21&curr=rub&dest=-455203&emp=0&lang=ru&locale=ru&page={page}&pricemarginCoeff=1.0&reg=1&regions=80,64,38,4,83,33,68,70,69,30,86,75,40,1,66,31,48,110,22,71&sort=popular&spp=27&sppFixGeo=4' % brand_id
            print(brand_id)
            products += await self.get_catalog(url=url)

        products = products[:5] + products[-5:]
        return products

    async def get_detail_by_nms(self, nms):
        output_data = []
        tasks = []
        count = 1

        for nm in nms:
            task = asyncio.create_task(self.get_detail(nm=nm))
            tasks.append(task)
            count += 1

            if count % 50 == 0:
                print(count, 'product data')
                output_data += await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

        output_data += await asyncio.gather(*tasks, return_exceptions=True)
        output_data = [item for item in output_data if not isinstance(item, Exception) and item]
        return output_data

    async def get_catalog(self, url):
        products = []

        for page in range(1, 101):
            print(page, 'catalog page')
            page_url = url.format(page=page)
            data = await self.make_get_request(page_url, headers={})

            if data:
                data = data['data']['products']
                products += data
                if len(data) != 100:
                    break
        return products


class APIUtils(BaseUtils):

    @staticmethod
    def auth(token):
        return {
            'Authorization': token
        }

    async def get_products_by_vendor_codes(self, token_auth, vendorCodes):
        output_data = []
        url = 'https://suppliers-api.wildberries.ru/content/v1/cards/filter'

        times = len(vendorCodes) // 100
        start = 0
        for i in range(times + 1):
            chunk_vendorCodes = vendorCodes[start: start + 100] if i != times else vendorCodes[start: len(vendorCodes) + 1]
            start += 100
            data = await self.make_post_request(url=url, payload=dict(vendorCodes=chunk_vendorCodes),
                                                headers=token_auth)
            if data:
                output_data += data.get('data', [])
        return output_data

    async def update_kts(self, token_auth, kts):
        print(len(kts), 'len kts')
        output_data = []
        url = 'https://suppliers-api.wildberries.ru/content/v1/cards/filter'

        times = len(kts) // 100
        start = 0
        for i in range(times + 1):
            chunk_kts = kts[start: start + 100] if i != times else kts[start: len(kts) + 1]
            start += 100
            data = await self.make_post_request(url=url, payload=dict(vendorCodes=chunk_kts),
                                                headers=token_auth)
            if data:
                output_data += data.get('data', [])
        return output_data

    async def get_products(self, token_auth):
        data = []
        url = 'https://suppliers-api.wildberries.ru/content/v1/cards/cursor/list'
        payload = {
            "sort": {
                "cursor": {
                    "limit": 1000
                },
                "filter": {
                    "withPhoto": -1
                }
            }
        }
        total = 1
        while total != 0:
            partial_data = await self.make_post_request(url=url, payload=payload, headers=token_auth)
            data += partial_data['data']['cards']
            cursor = partial_data['data']['cursor']
            payload['sort']['cursor'].update(cursor)
            total = cursor['total']
        return data


def make_head(article: int):
    head = 'https://basket-{i}.wb.ru'

    if article < 14400000:
        number = '01'
    elif article < 28800000:
        number = '02'
    elif article < 43500000:
        number = '03'
    elif article < 72000000:
        number = '04'
    elif article < 100800000:
        number = '05'
    elif article < 106300000:
        number = '06'
    elif article < 111600000:
        number = '07'
    elif article < 117000000:
        number = '08'
    elif article < 131400000:
        number = '09'
    else:
        number = '10'
    return head.format(i=number)


def make_tail(article: str, item: str):
    length = len(str(article))
    if length <= 3:
        return f'/vol{0}/part{0}/{article}/info/' + item
    elif length == 4:
        return f'/vol{0}/part{article[0]}/{article}/info/' + item
    elif length == 5:
        return f'/vol{0}/part{article[:2]}/{article}/info/' + item
    elif length == 6:
        return f'/vol{article[0]}/part{article[:3]}/{article}/info/' + item
    elif length == 7:
        return f'/vol{article[:2]}/part{article[:4]}/{article}/info/' + item
    elif length == 8:
        return f'/vol{article[:3]}/part{article[:5]}/{article}/info/' + item
    else:
        return f'/vol{article[:4]}/part{article[:6]}/{article}/info/' + item
