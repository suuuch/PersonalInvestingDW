from sqlalchemy import create_engine, text

from config import SQLALCHEMY_DATABASE_URI

engine = create_engine(SQLALCHEMY_DATABASE_URI,
                       # echo=True,
                       pool_size=8,
                       pool_recycle=60 * 30,
                       pool_pre_ping=True)
