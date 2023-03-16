from sqladmin import ModelView
from ssp_management.models import Brand


class BrandAdmin(ModelView, model=Brand):
    column_list = ['id', 'title', 'brand_ids']