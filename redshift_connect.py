
###TESTING CONNECTION TO OUR AMAZON REDSHIFT DATABASE####

import configparser
%load_ext sql
import psycopg2
import os 

config = ConfigParser()
config.read_file(open('credentials.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("CLUSTER","DWH_DB")
DWH_DB_USER= config.get("CLUSTER","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DWH_DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DWH_PORT")

# FILL IN YOUR REDSHIFT ENDPOINT HERE
DWH_ENDPOINT=""
    
#FILL IN YOUR IAM ROLE ARN
DWH_ROLE_ARN=""



conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string


### Extract / Check from different tables from the UK- Accident-Data Star Schema###

%%sql
SELECT * FROM condition
LIMIT 5