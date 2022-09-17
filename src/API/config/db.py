import yaml
import os
import sqlalchemy as db

class ConnectionPostgresql():

    def __init__(self):
        self.db_name = 'SalesSA'
    
    def get_conn(self):
        #reading yml file
        yaml_file = open(f"{os.getcwd()}/config/credentials.yaml")

        #Convert to JSON
        parsed_yaml_file = yaml.load(yaml_file, Loader=yaml.FullLoader)

        my_connection_user = parsed_yaml_file["postgresql_development"]["username"]
        my_connection_password = parsed_yaml_file["postgresql_development"]["password"]

        conn_var =  self.connect_to_postgreDB(my_connection_user,my_connection_password,self.db_name)

        return conn_var   
    def connect_to_postgreDB(self,user,password,db_name):
            #creating engine for connection
            engine = db.create_engine(f'postgresql://{user}:{password}@localhost:5432/{db_name}')

            #connecting to PostgreSQL
            conn = engine.connect()
            return conn