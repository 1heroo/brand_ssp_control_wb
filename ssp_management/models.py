from db.db import Base
from sqlalchemy.dialects.postgresql import JSONB
import sqlalchemy as sa


class Brand(Base):
    __tablename__ = 'brands'

    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.String)
    brand_ids = sa.Column(JSONB)

    def __str__(self):
        return self.title

    def __repr__(self):
        return self.title


class MyProduct(Base):
    __tablename__ = 'my_product'

    id = sa.Column(sa.Integer, primary_key=True)
    nm_id = sa.Column(sa.Integer)
    brand = sa.Column(sa.String)

    def __str__(self):
        return str(self.nm_id)

    def __repr__(self):
        return str(self.nm_id)
