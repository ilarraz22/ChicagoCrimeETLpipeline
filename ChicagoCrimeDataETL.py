import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine

#scrape the data
url = 'https://data.cityofchicago.org/resource/ijzp-q8t2.json'

response = requests.get(url)


if response.status_code == 200:
    print("Data retrieved successfully!")
else:
    print(f"Error: {response.status_code}")
    print(response.text) 
    
#create the dataframe for the json
data = response.json()
df = pd.DataFrame(data)
df = df.iloc[:, :22]

#changes date columns to datetime
df['date'] = pd.to_datetime(df['date'])
df['updated_on'] = pd.to_datetime(df['updated_on'])

#cleans up the location column from a list to a simple tuple that matches what is in the database
df.drop(['location'], axis = 1, inplace=True)
df['location'] = list(zip(df['latitude'], df['longitude']))

#send the data to the database
engine = create_engine('postgresql://your_username:your_password@your_localhostorserverip:your_port/your_database')

df.to_sql('ChicagoCrimeData', engine, if_exists='append', index= False)