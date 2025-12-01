from sqlalchemy.orm import declarative_base
from sqlalchemy import MetaData

small_business_data_base = declarative_base(metadata=MetaData(schema="small_business_data"))
