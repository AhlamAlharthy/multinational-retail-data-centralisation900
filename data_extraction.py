#This class will work as a utility class, in it you will be creating methods that help extract data from different data sources
#The methods contained will ve fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket
#%%
class DataExtractor:
    def read_rds_table(self, db_connector, table_name): #this will extract the database table to a panda DataFrame. db_connector is an instance on the DatabaseConnector class.
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df

    def table(self, dbconnector, table_name):
        df = pd.read_sql_table(table_name, dbconnector, index_col="index")
        return df 
# %%
x = DataExtractor()
# %%
