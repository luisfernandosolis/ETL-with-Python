from re import I
from database.db import ConnectionPostgresql
import pandas as pd


class RetailSaReports():
    def __init__(self,schema_name_raw,conn_var):
        self.df_deparment = pd.read_sql(f'select * from {schema_name_raw}.department', con=conn_var)
        self.df_orderItem = pd.read_sql(f'select * from {schema_name_raw}."orderItem"', con=conn_var)
        self.df_order = pd.read_sql(f'select * from {schema_name_raw}."order"', con=conn_var)
        self.df_category = pd.read_sql(f'select * from {schema_name_raw}.category', con=conn_var)
        self.df_customer = pd.read_sql(f'select * from {schema_name_raw}.customer', con=conn_var)
        self.df_product = pd.read_sql(f'select * from {schema_name_raw}.product', con=conn_var)


    def get_departments_income(self):
        department_category=self.df_deparment.merge(self.df_category, left_on="department_id", right_on="category_department_id", how="left")
        category_product=department_category.merge(self.df_product, left_on="category_id", right_on="product_category_id", how="left")
        producto_orderItem=category_product.merge(self.df_orderItem, left_on="product_id", right_on="order_item_product_id", how="left")
        df_result=producto_orderItem.groupby("department_name")["order_item_subtotal"].sum().reset_index().sort_values(by="order_item_subtotal", ascending=True)
        return df_result
    def get_categories_purchases(self):
        category_product=self.df_category.merge(self.df_product, left_on="category_id", right_on="product_category_id", how="left")
        producto_orderItem=category_product.merge(self.df_orderItem, left_on="product_id", right_on="order_item_product_id", how="left")
        df_result=producto_orderItem.groupby("category_name")["order_item_subtotal"].sum().reset_index().sort_values(by="order_item_subtotal", ascending=True)
        df_result["order_item_subtotal"]=df_result["order_item_subtotal"].astype("int")
        df_result.columns = ['category_name','total_quantity']
        return df_result
    def get_customers_purchases(self):
        customer_order=self.df_customer.merge(self.df_order, left_on="customer_id", right_on="order_customer_id", how="left")
        order_orderitem=customer_order.merge(self.df_orderItem, left_on="order_id", right_on="order_item_order_id", how="left")
        df_result=order_orderitem.groupby(["customer_fname","customer_lname"])["order_item_subtotal"].sum().reset_index().sort_values(by="order_item_subtotal", ascending=True)
        df_result["order_item_subtotal"]=df_result["order_item_subtotal"].astype("int")
        df_result.columns = ['customer_fname',"customer_lname",'total_quantity']
        df_result=df_result.sort_values(by="total_quantity", ascending=False)
        return df_result.head(10)

if __name__=="__main__":
    objectConn = ConnectionPostgresql()
    conn_var = objectConn.get_conn()

    #Getting connection variable
    schema_name_raw = 'raw_data'
    schema_name_landing = 'landing'


    ## get reports
    sales_sa=RetailSaReports(schema_name_raw,conn_var)
    departments_income=sales_sa.get_departments_income()
    categories_income=sales_sa.get_categories_purchases()
    customer_income=sales_sa.get_customers_purchases()

    # insert to database
    departments_income.to_sql('department_total_income', con=conn_var, if_exists='replace',index = False,schema =schema_name_landing)
    categories_income.to_sql('category_total_income', con=conn_var, if_exists='replace',index = False,schema =schema_name_landing)
    customer_income.to_sql('customer_total_income', con=conn_var, if_exists='replace',index = False,schema =schema_name_landing)



    