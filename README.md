# Technical-test
Extracting data from CSV and API
Transforming- Cleaning and Formatting the data
Loading- Ingesting it into the database

<div style="padding: 0; margin: 0; line-height: 1.2;">
    <h1 style="margin: 0;"><b><font color='blue'>Performing ETL on Small Data</font></b></h1>
    <p style="margin: 0;"><b>1. Exploratory Analysis</b><br>
    <p style="margin: 0;"><b>2. Connecting to the Database</b><br>
    - Creating a New Table.</p>
    <p style="margin: 0;"><b>3. Collecting Data from Different Sources</b><br>
    - Uploading the Data from a CSV file<br>
    <p style="margin: 0;"><b>4. Transforming the data</b><br>
    <p style="margin: 0;"><b>5. Using API to Pull the Exchange Rate Data for Each Market from the Web</b><br>
    <p style="margin: 0;"><b>5. Updating the Table in the Database</b><br>
</div>

#conda update jupyter
#%load_ext sql
#pip install psycopg2
#pip install sqlalchemy
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import types
from sqlalchemy import create_engine
<font color='blue'>Loading CSV in Dataframe</font>
df = pd.read_csv (r'C:\Users\Ashwini\Ably\transactions.csv')   

df.head()
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['timestamp'] = df['timestamp'].dt.strftime('%d/%m/%Y')
print(df)
<font color='blue'>Connecting to Postgresql Database</font>
try:
    conn = psycopg2.connect("dbname='mydb' user='postgres' host='localhost' password='Ashwini@12'")
except:
    print("Database Connection Error")
conn.autocommit = True
cursor = conn.cursor()
<font color='blue'>Loading data into the Database</font>
cursor.execute("DROP TABLE IF EXISTS transaction_raw")
engine = create_engine('postgresql://postgres:Ashwini@12@localhost/mydb')
df.to_sql('transaction_raw',con= engine,if_exists='replace',index=False,
           dtype ={'transaction_id': sqlalchemy.types.VARCHAR(50),
                     'user' : sqlalchemy.types.INTEGER,
                     'timestamp' :sqlalchemy.types.Date,
                     'tz' :sqlalchemy.types.VARCHAR(length=20),
                     'currency':sqlalchemy.types.VARCHAR(length=10),
                     'value' :sqlalchemy.types.Numeric,
                     'cleared' :sqlalchemy.types.Boolean,
                     'country':sqlalchemy.types.VARCHAR(length=50)})
cursor.execute("DROP TABLE IF EXISTS transaction")
sql="""Create Table transaction as 
        Select * from transaction_raw
        where value is not null"""
cursor.execute(sql)
<font color='blue'>Transforming the data</font>
sql = """SELECT table_name, column_name, data_type 
         FROM information_schema.columns
         WHERE table_name = 'transaction'"""
cursor.execute(sql)
for i in cursor.fetchall():
    print(i)
s = """ALTER TABLE transaction
       ALTER COLUMN value TYPE Numeric(10,2);
       """
cursor.execute(s)
up = """UPDATE transaction
        SET currency = 'EUR' 
        WHERE currency IN ('Euro')"""
cursor.execute(up)
upd = """UPDATE transaction
        SET currency = 'USD' 
        WHERE currency IN ('US Dollars')"""
cursor.execute(upd)
s0 = """UPDATE transaction
        SET currency = 'GBP' 
        WHERE currency isnull"""
cursor.execute(s0)
s2 = """UPDATE transaction
        SET country = 'United Kingdom'
        where country is null"""
cursor.execute(s2)

s3 = """select * from transaction where country is null
        """
cursor.execute(s3)
for i in cursor.fetchall():
    print(i)
<font color='blue'>Extracting from API</font>
import json,requests
response = requests.get('https://v6.exchangerate-api.com/v6/435a57dc789ddcb46282a1b6/latest/GBP')
c_rates = response.text
rates_d = json.loads(c_rates)
rates_d
cursor.execute("DROP TABLE IF EXISTS exchange_rate")
sql4 = """create table exchange_rate(currency varchar, rate Numeric(10,2))"""
cursor.execute(sql4)
sql6 = """INSERT INTO exchange_rate VALUES(%s, %s)"""

for record in rates_d["conversion_rates"].items():
    cursor.execute(sql6, [record[0], record[1]])

sql9 = """select * from exchange_rate """
cursor.execute(sql9)
for i in cursor.fetchall():
    print(i)
sql10 = """SELECT table_name, column_name, data_type 
           FROM information_schema.columns
           WHERE table_name = 'exchange_rate'"""
cursor.execute(sql10)
for i in cursor.fetchall():
    print(i)
