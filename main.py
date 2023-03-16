import uvicorn
from fastapi import FastAPI

from db.db import async_engine
from ssp_management.admin import BrandAdmin
from sqladmin import Admin
from ssp_management.routes import router as sm_router


app = FastAPI()


app.include_router(sm_router)
admin = Admin(app, async_engine)

admin.add_view(BrandAdmin)


if __name__ == '__main__':
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=8000,
        reload=True
    )
