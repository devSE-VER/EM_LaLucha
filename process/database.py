import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text, Engine


def get_env_engine():
    load_dotenv()
    cnx_str = "mssql+pyodbc://{user}:{password}@{server}/{db}?driver={dvr}".format(
        server = os.getenv('SQL_SERVER'),
        user = os.getenv('SQL_USER'),
        password = quote_plus(os.getenv('SQL_PASSWORD')),
        db = os.getenv('SQL_DB'),
        dvr = os.getenv('SQL_DRIVER'),
    )
    engine = create_engine(cnx_str, echo=False)
    print("Connected!")
    return engine


def check_file_exist(fname:str, cnx_str:str, schema:str, table:str):
    engine = create_engine(cnx_str, echo=False)
    
    qry_str = text(f"select fname from {schema}.{table} where fname = '{fname}'")

    with engine.connect() as conn:
        file_exist = conn.execute(qry_str).scalar()
        conn.close()
    
    return bool(file_exist)
