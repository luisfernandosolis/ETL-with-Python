from fastapi import APIRouter
from config.db import ConnectionPostgresql
import pandas as pd
import json

object_conn=ConnectionPostgresql()
conn_var = object_conn.get_conn()
schema_name_landing="landing"

router=APIRouter()

@router.get("/")
async def root():
    return "hola mundo"

@router.get("/department_total_income")
def department_total_income():
    df_department_total_income=pd.read_sql(f'select * from {schema_name_landing}.department_total_income', con=conn_var)
    result=json.loads(json.dumps(list(df_department_total_income.T.to_dict().values())))

    return result

@router.get("/category_total_income")
def category_total_income():
    df_department_total_income=pd.read_sql(f'select * from {schema_name_landing}.category_total_income', con=conn_var)
    result=json.loads(json.dumps(list(df_department_total_income.T.to_dict().values())))

    return result

@router.get("/customer_total_income")
def customer_total_income():
    df_department_total_income=pd.read_sql(f'select * from {schema_name_landing}.customer_total_income', con=conn_var)
    result=json.loads(json.dumps(list(df_department_total_income.T.to_dict().values())))

    return result