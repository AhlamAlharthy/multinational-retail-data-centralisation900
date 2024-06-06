#%%
#This will be used to connect with and upload data to the database
class DatabaseConnector:

    def read_db_creds(self, creds_file): #reads the credentials from the yaml file and returns it as a dictionary 
        with open(creds_file, 'r') as file: #Opens the file db-creds.yaml in read mode
            creds = yaml.safe_load(file) 
            return creds

    def init_db_engine(self): #Uses the credentials to create and return an SQLAlchemy engine
        creds = self.read_db_creds() #Calls read_db_creds to get the credentials
        conn = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}" #Constructs a connection string using the credentials
        engine = create_engine(conn) #Creates an SQLAlchemy engine using the connection string
        return engine


    def list_db_tables(self): #lists all the table in the database
        engine = self.init_db_engine() #Calls init_db_engine to get the engine
        inspector = inspect(engine) #Uses SQLAlchemy's inspect function to create an inspector object
        tables_info = {} #Initializes an empty dictionary tables_info to store table information

        for table_name in inspector.get_table_names(): #Loops through the table names obtained from inspector.get_table_names()
            columns = [column['name'] for column in inspector.get_columns(table_name)] #For each table, retrieves the columns
            tables_info[table_name] = columns
            print(f"Table: {table_name}")
            for column in columns:
                print(f"  Column: {column}")

        return tables_info


    def upload_to_db(self, df, table_name): #Uploads a Pandas DataFrame to the database
        engine = self.init_db_engine() # calls init_db_engine to get the engine 
        df.to_sql(table_name, engine, if_exists='replace', index=False) #uploads DataFrame to the database
        print(f"Data uploaded to table {table_name} successfully.")
        
    def view_first_entries(self, table_name, num_entries=5):
        """Fetches and prints the first few entries of the specified table"""
        engine = self.init_db_engine()
        query = f"SELECT * FROM {table_name} LIMIT {num_entries}"
        df = pd.read_sql(query, engine)
        print(df.head(num_entries))
        return df.head(num_entries)   
        
# %%

x = DatabaseConnector()
y = x.list_db_tables()
# %%
