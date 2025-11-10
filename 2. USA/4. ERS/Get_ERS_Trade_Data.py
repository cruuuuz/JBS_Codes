import pandas
import sys
import requests
import os
import zipfile
import snowflake.connector, snowflake.connector.pandas_tools as spd
from datetime import datetime, timedelta
sys.path.append('O:/Cruz/Codes/Inteligência')
from password import password
from snowflake_connector import snowflake_connector


proxies = {
        'http' : f'http://{password()}@MTZSVMFCPPRD02:8080',
        'https' : f'http://{password()}@MTZSVMFCPPRD02:8080'
    }

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

base_path = 'O:/Cruz/Inteligencia/USA/USDA/ERS_API'

for arquivo in os.listdir(base_path):
    if arquivo.endswith('.csv'):
        caminho_arquivo = os.path.join(base_path, arquivo)
        os.remove(caminho_arquivo)
        print(f'{arquivo} removido')

zip_filename = f'{base_path}/Trade_Data_ERS.zip'
url = 'https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/81475/LivestockMeatTrade.zip?v=17944'
with requests.get(url, stream=True, proxies=proxies, verify=False) as r:
    r.raise_for_status()  # Verifica se houve algum erro na requisição
    with open(zip_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=10500): 
            f.write(chunk)
with zipfile.ZipFile(zip_filename, 'r') as r:
    r.extractall('O:/Cruz/Inteligencia/USA/USDA/ERS_API')
for arquivo in os.listdir(base_path):
    if arquivo.endswith('.zip'):
        caminho_arquivo = os.path.join(base_path, arquivo)
        os.remove(caminho_arquivo)
        print(f'{arquivo} removido')


def parse_df(df):
    df_parsed = df
    df_parsed.columns = ['SOURCE_ID', 'HS_CODE', 'COMMODITITY_DESCRIPTION', 'COUNTRY_CODE', 'COUNTRY_NAME', 'ATTRIBUTE', 'UNIT', 'YEAR', 'MONTH', 'VALUE']
    df_parsed['DATE'] = df_parsed['MONTH'].astype(str) + "/"+ df_parsed['YEAR'].astype(str)
    df_parsed['DATE'] = pandas.to_datetime(df_parsed['DATE'])
    df_parsed = df_parsed[['DATE', 'YEAR', 'MONTH', 'COMMODITITY_DESCRIPTION', 'HS_CODE', 'COUNTRY_NAME', 'COUNTRY_CODE', 'ATTRIBUTE', 'UNIT', 'VALUE', 'SOURCE_ID']]
    df_parsed['VALUE'] = pandas.to_numeric(df_parsed['VALUE'])
    df_parsed['DATE'] = pandas.to_datetime(df_parsed['DATE']).dt.strftime('%Y-%m-%d')
    return df_parsed

def send_data_SF(df, type: str):
    if type == 'e':
        df = df[df['ATTRIBUTE'] == 'US Export, QTY']
        last_date = df['DATE'].max()
        last_date_snowflake = cursor.execute(f"SELECT MAX(DATE) FROM USDA_ERS_TRADE WHERE ATTRIBUTE = 'US Export, QTY'")
        result_date = cursor.fetchall()
        date_snowflake = str(result_date[0][0])
        consulta_last_date = cursor.execute(f"SELECT * FROM USDA_ERS_TRADE WHERE DATE = '{last_date}' AND ATTRIBUTE = 'US Export, QTY'") 
        results = cursor.fetchall()
        if results==[]:
            df_to_snowflake = df.loc[df['DATE'] > date_snowflake]
            x = spd.write_pandas(cnn, df_to_snowflake, table_name="USDA_ERS_TRADE")
            print(f'Data about ERS Trade EXPORTS at date {last_date} was updated')
        else:
            print(f'Data about ERS Trade EXPORTS at date {last_date} already exists')
    else:
        df = df[df['ATTRIBUTE'] == 'US Import, QTY']
        last_date = df['DATE'].max()
        last_date_snowflake = cursor.execute(f"SELECT MAX(DATE) FROM USDA_ERS_TRADE WHERE ATTRIBUTE = 'US Import, QTY'")
        result_date = cursor.fetchall()
        date_snowflake = str(result_date[0][0])
        consulta_last_date = cursor.execute(f"SELECT * FROM USDA_ERS_TRADE WHERE DATE = '{last_date}' AND ATTRIBUTE = 'US Import, QTY'") 
        results = cursor.fetchall()
        if results==[]:
            df_to_snowflake = df.loc[df['DATE'] > date_snowflake]
            x = spd.write_pandas(cnn, df_to_snowflake, table_name="USDA_ERS_TRADE")
            print(f'Data about ERS Trade IMPORTS at date {last_date} was updated')
        else:
            print(f'Data about ERS Trade IMPORTS at date {last_date} already exists')


#DataFrame_Exports
df_exports = pandas.read_csv(f'{base_path}/LivestockMeat_Exports.csv')
df_exports = parse_df(df_exports)
send_data_SF(df_exports, 'e')

#DataFrame_Imports
df_imports = pandas.read_csv(f'{base_path}/LivestockMeat_Imports.csv')
df_imports = parse_df(df_imports)
send_data_SF(df_imports, 'i')
