from sqladmin import ModelView
from ssp_management.models import Brand, MyProduct


class BrandAdmin(ModelView, model=Brand):
    column_list = ['id', 'title', 'brand_ids']


class ProductAdmin(ModelView, model=MyProduct):
    column_list = ['id', 'nm_id', 'brand']

