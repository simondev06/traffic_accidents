import os
from sqlalchemy import create_engine
def get_mysql_connection():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)
