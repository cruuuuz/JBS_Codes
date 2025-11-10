import pandas
import snowflake.connector
import snowflake.connector.pandas_tools as spd
import sys
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


def send_weather_data(df_final):
    last_date = df_final['DATE'].max()

    max_date_from_snowflake = cursor.execute(f"SELECT MAX(DATE) FROM USA_WEATHER_MONTHLY_DATA")
    max_date_from_snowflake = cursor.fetchall()
    max_date_from_snowflake = str(max_date_from_snowflake[0][0])
    
    cursor.execute(f"SELECT * FROM USA_WEATHER_MONTHLY_DATA WHERE DATE = '{last_date}'")
    results = cursor.fetchall()

    if results == []:
        df_to_snowflake = df_final[df_final['DATE'] > f'{max_date_from_snowflake}']
        spd.write_pandas(cnn, df_to_snowflake, table_name="USA_WEATHER_MONTHLY_DATA")
        print(f'Monthly weather data until {last_date} is updated')
    else:
        print(f'Monthly weather data until {last_date} already exists')
