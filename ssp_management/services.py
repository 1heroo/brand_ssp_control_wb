import pandas as pd

from core.settings import settings
from ssp_management.models import Brand
from ssp_management.queries import BrandQueries, MyProductQueries
from ssp_management.utils import ParsingUtils, APIUtils


class SSPServices:

    def __init__(self):
        self.brand_queries = BrandQueries()
        self.parsing_utils = ParsingUtils()
        self.my_product_queries = MyProductQueries()
        self.api_utils = APIUtils()

    async def ssp_management(self):
        brands = await self.brand_queries.fetch_all()
        my_product_nms = [product.nm_id for product in await self.my_product_queries.fetch_all()]
        my_products_json = await self.parsing_utils.get_detail_by_nms(nms=my_product_nms)

        for brand in brands:
            df = await self.get_ssp_table(brand=brand)
            await self.manage_ssp(brand_ids=brand.brand_ids, products=my_products_json, ssp_table_df=df)

    async def manage_ssp(self, brand_ids: list[int], products: list[dict], ssp_table_df: pd.DataFrame):
        products = [product for product in products if product['detail']['brandId'] in brand_ids]
        auth = self.api_utils.auth(token=settings.WB_API_TOKEN)
        kts_to_be_updated = []

        for product in products:
            subj_name = product['card'].get('subj_name')
            subj_root_name = product['card'].get('subj_root_name')
            extended = product['detail'].get('extended')
            if not extended:
                continue
            my_product_ssp = extended.get('clientSale')

            for index in ssp_table_df.index:
                if ssp_table_df['Подкатегория'][index] == subj_root_name \
                        and ssp_table_df['Подкатегория2'][index] == subj_name and my_product_ssp:

                    brand_column, max_ssp = self.parsing_utils.find_max_ssp_column(df=ssp_table_df, index=index)

                    if max_ssp > my_product_ssp:
                        print(f'found product to be updated', product['card'].get('nm_id'))
                        kts_to_be_updated.append(
                            await self.update_brand(product=product, max_spp=max_ssp, auth=auth))
        await self.api_utils.update_kts(token_auth=auth, kts=kts_to_be_updated)

    async def update_brand(self, product, max_spp, auth):

        ssp, brand_name = max_spp.split(' ')
        vendorCode = product['card'].get('vendor_code')
        print(vendorCode)
        products = await self.api_utils.get_products_by_vendor_codes(token_auth=auth, vendorCodes=[vendorCode])
        product = products[0]

        for characteristic in product.get('characteristics', []):
            for key, value in characteristic.items():
                if key == 'Бренд':
                    characteristic[key] = brand_name
        return product

    async def get_ssp_table(self, brand: Brand):
        print(brand.brand_ids)
        products = await self.parsing_utils.get_products(brand_ids=brand.brand_ids)

        all_unique_products: list[list] = []
        for brand_id in brand.brand_ids:
            categories = []
            unique_products = []

            for product in products:
                extended = product['detail'].get('extended')
                category = product['card'].get('subj_name')
                if category not in categories:

                    if not extended:
                        continue
                    categories.append(category)
                    unique_products.append({
                        'Подкатегория': product['card'].get('subj_root_name'),
                        'Подкатегория2': product['card'].get('subj_name'),
                        f'ssp {brand_id}': f'{extended.get("clientSale")} {product["detail"].get("brand")}',
                        # f'nm {brand_id}': product['detail'].get('id')
                    })
                    products.remove(product)
            all_unique_products.append(unique_products)

        df_list = [pd.DataFrame(product_list) for product_list in all_unique_products]

        df_initial = df_list[0]
        for df in df_list[1:]:
            df_initial = pd.merge(df_initial, df, how='outer',
                                  left_on=['Подкатегория', 'Подкатегория2'],
                                  right_on=['Подкатегория', 'Подкатегория2'])
        return df_initial.fillna(0)

    async def collect_bland_products_in_db(self):
        auth = self.api_utils.auth(token=settings.WB_API_TOKEN)
        products = await self.api_utils.get_products(token_auth=auth)
        # brands = set()
        # for product in products:
        #     brands.add(product.get('brand'))
        products_to_be_saved = []
        for product in products:
            if product.get('brand') in ['Sturm!', 'P.I.T.']:
                products_to_be_saved.append(MyProductQueries.model(
                    nm_id=product.get('nmID'),
                    brand=product.get('brand'),
                ))
        await self.my_product_queries.save_in_db(instances=products_to_be_saved, many=True)

