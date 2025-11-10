import sys
import snowflake.connector, snowflake.connector.pandas_tools as spd
sys.path.append('O:/Cruz/Codes/Inteligência')
from snowflake_connector import snowflake_connector

snowflake_connector = snowflake_connector()
cnn = snowflake.connector.connect(
user = snowflake_connector['user'],
password = snowflake_connector['password'],
account = snowflake_connector['account'],
proxy_host = snowflake_connector['proxy_host'],
proxy_port = snowflake_connector['proxy_port']
)
cursor = cnn.cursor()

x = cursor.execute('USE DATABASE DB_BR_RISKMANAGEMENT')
x = cursor.execute('USE SCHEMA MID')
x = cursor.execute('USE WAREHOUSE WH_BR_TEAMRISK_XS')

def send_weather_data(df, variable_type):
    if variable_type == 'prcp':
        variable_full_name = 'Precipitation'
    elif variable_type == 'tavg':
        variable_full_name = 'Temperature_Avg'
    elif variable_type == 'tmax':
        variable_full_name = 'Temperature_Max'
    else:
        variable_full_name = 'Temperature_Min'

    last_date = df['DATE'].max()
    last_date_snowflake = cursor.execute(f"SELECT MAX(DATE) FROM US_WEATHER_DATA WHERE VARIABLE_TYPE = '{variable_full_name}'")
    result_date = cursor.fetchall()
    date_snowflake = str(result_date[0][0])
    consulta_last_date = cursor.execute(f"SELECT * FROM US_WEATHER_DATA WHERE DATE = '{last_date}' AND VARIABLE_TYPE = '{variable_full_name}'") 
    results = cursor.fetchall()
    if results==[]:
        df_to_snowflake = df.loc[df['DATE'] > date_snowflake]
        x = spd.write_pandas(cnn, df_to_snowflake, table_name="US_WEATHER_DATA")
        print(f'Data about {variable_full_name} at date {last_date} was updated')
    else:
        print(f'Data about {variable_full_name} at date {last_date} already exists')