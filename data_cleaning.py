#This containes methods to clean data from each of the data sources
class DataCleaning:
    def clean_user_data(self,df): #This will perform the cleaning for the user data

        #drop all rows with null value
        df.dropna(inplace=True) 

        # Convert date columns to datetime format if they exist
        if 'birthdate' in df.columns:
            df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
        if 'registration_date' in df.columns:
            df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')

        return df
        