# %%
import psycopg2
import pandas as pd
import yaml

from sqlalchemy import create_engine
from sqlalchemy import inspect

#%%
def upload_dim_users():
    extracting = DataExtractor()    # Create DataExtractor instance
    db_connector= DatabaseConnector() # Create instance of DatabaseConnector
    cleaning = DataCleaning()      # Create instance of DataCleaning
    
    # Connect to remote db and create dataframe
    read_cred = db_connector.read_db_creds("remote-db-creds.yaml")
    df2 = extracting.read_rds_table(db_connector, 'legacy_users') # Create dataframe from remote DB
    df2 = cleaning.clean_user_data(df2) # Clean dataframe
    
    # Upload the cleaned dataframe to the local db
    read_cred_local = db_connector.read_db_creds("local-db-creds.yaml")
    db_connector.upload_to_db(df2, 'dim_users', read_cred_local) # Upload dataframe to local DB

# Call the function to execute the process
upload_dim_users()


# %%
