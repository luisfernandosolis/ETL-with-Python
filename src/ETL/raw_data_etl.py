import pandas as pd
import os
import datetime

from utils.DateFormat import DateFormat
from database.db import ConnectionPostgresql

schema_name = 'raw_data'
PATH_DATA=os.getcwd() +"/database/sales_sa"

from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)

# Get data to load a raw_data
def get_data(path_sets):
    df_data={
        "df_categories":pd.read_csv(path_sets["categories"],names=['category_id','category_department_id','category_name'], sep="|"),
        "df_customer":pd.read_csv(path_sets["customer"],names=['customer_id','customer_fname','customer_lname','customer_email','customer_password','customer_street','customer_city','customer_state','customer_zipcode'], sep="|"),
        "df_departments":pd.read_csv(path_sets["departments"],names=['department_id','department_name'],sep="|"),
        "df_order_items":pd.read_csv(path_sets["order_items"],names=['order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price'], sep="|"),
        "df_orders":pd.read_csv(path_sets["orders"],names=['order_id','order_date','order_customer_id','order_status'], sep="|"),
        "df_products":pd.read_csv(path_sets["products"],names=['product_id','product_category_id','product_name','product_description','product_price','product_image'], sep="|")
    }
    return df_data
    

##prep data before load to raw_data
def prep_data(dataframes):
    ## categories
    df_categories=dataframes["df_categories"]


    ## customer
    df_customer=dataframes["df_customer"]
    df_customer["customer_zipcode"] = df_customer["customer_zipcode"].astype('str').str.zfill(5)

    ## departmets
    df_departments=dataframes["df_departments"]

    ## order_items
    df_order_items=dataframes["df_order_items"]

    # orders
    df_orders=dataframes["df_orders"]
    #df_orders['order_date'] = df_orders['order_date'].apply(lambda date: DateFormat.convert_date(date))


    # products
    df_products=dataframes["df_products"]
    df_products = df_products.loc[:, ['product_id','product_category_id','product_name','product_price','product_image']]


    df_data_prep={
        "df_categories":df_categories,
        "df_customer":df_customer,
        "df_departments":df_departments,
        "df_order_items":df_order_items,
        "df_orders":df_orders,
        "df_products":df_products
    }
    return df_data_prep

# load data prep into postgresql raw_data
def load_raw_data():

    ## Path to datasets
    PATH_DATASET={
        "categories":PATH_DATA+"/categories",
        "customer":PATH_DATA+"/customer",
        "departments":PATH_DATA+"/departments",
        "order_items":PATH_DATA+"/order_items",
        "orders":PATH_DATA+"/orders",
        "products":PATH_DATA+"/products", 
    }

    ## load data to DataFrame
    #df_categories,df_customer,df_departments,df_order_items,df_orders,df_products =get_data(path_sets=PATH_DATASET)

    #step 2 : prep data into insert to Database

    df_data_ready=prep_data(get_data(path_sets=PATH_DATASET))


    #step 3
    object_conn=ConnectionPostgresql()
    conn_var = object_conn.get_conn()
    print(conn_var)

    #loading DF data to postgre

    df_data_ready["df_categories"].to_sql('category', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_data_ready["df_customer"].to_sql('customer', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_data_ready["df_departments"].to_sql('department', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_data_ready["df_order_items"].to_sql('orderItem', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_data_ready["df_orders"].to_sql('order', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_data_ready["df_products"].to_sql('product', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    conn_var.close()


if __name__=="__main__":
    load_raw_data()
