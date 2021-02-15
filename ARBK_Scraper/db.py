import sys
from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine


meta = MetaData()
engine = create_engine(f"postgresql+psycopg2://{sys.argv[1]}:{sys.argv[2]}@localhost/arbk")


business = Table(
    'Businesses', meta,
    Column('ID', Integer, primary_key=True),
    Column('Name', String, nullable=False),
    Column('Trade_Name', String),
    Column('Business_ID', Integer),
    Column('Region', String),
    Column('Business_Type', String),
    Column('Status', String),
    Column('Link', String)
)

business_data = Table(  # Data are stored in Key-Value pairs
    'BusinessInfo', meta,
    Column('ID', Integer, primary_key=True),
    Column('Business_ID', Integer),
    Column('Key', String),
    Column('Value', String)
)

activity = Table(
    'Activities', meta,
    Column('ID', Integer, primary_key=True),
    Column('Business_ID', Integer),
    Column('Description', String),
    Column('Type', String)
)


def create_tables():
    meta.create_all(engine)
