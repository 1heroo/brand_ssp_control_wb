import io

import pandas as pd
from fastapi import APIRouter
from starlette.responses import StreamingResponse

from ssp_management.queries import BrandQueries
from ssp_management.services import SSPServices

router = APIRouter(prefix='/ssp-management', tags=['main'])

brand_queries = BrandQueries()
ssp_services = SSPServices()


@router.get('/')
async def main():
    await ssp_services.ssp_management()


@router.get('/aggregate-nms/')
async def aggregate_products():
    await ssp_services.collect_bland_products_in_db()


@router.get('/ssp-table/{brand_id}/')
async def get_ssp_table(brand_id: int):
    brand = await brand_queries.get_brand_by_id(brand_id=brand_id)
    df = await ssp_services.get_ssp_table(brand=brand)

    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False)
    writer.save()

    return StreamingResponse(io.BytesIO(output.getvalue()),
                             media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="ssp_table.xlsx"'})
