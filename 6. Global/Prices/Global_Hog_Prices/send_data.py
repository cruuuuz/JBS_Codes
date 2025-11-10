import pandas
import sys
import snowflake.connector
import snowflake.connector.pandas_tools as spd
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

path = 'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices'

df_europa = pandas.read_excel(f'{path}/Europe_Swine_Prices_Weekly.xlsx')
df_europa = pandas.melt(df_europa, id_vars=['DATES'], var_name='COUNTRY', value_name='PRICE')
df_europa['PRICE'] = df_europa['PRICE'].round(2)
df_europa['DATES'] = pandas.to_datetime(df_europa['DATES'], errors='coerce').dt.strftime("%Y-%m-%d")
df_europa = df_europa.dropna()
df_europa.columns = ['DATE', 'COUNTRY', 'PRICE']

df_americas = pandas.read_excel(f'{path}/Americas_Hogs_Prices.xlsx')
df_americas = pandas.melt(df_americas, id_vars=['Date'], var_name='COUNTRY', value_name='PRICE')
df_americas['PRICE'] = df_americas['PRICE'].round(2)
df_americas['COUNTRY'] = df_americas['COUNTRY'].str.upper()
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/0/','/01/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/1/','/02/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/2/','/03/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/3/','/04/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/4/','/05/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/5/','/06/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/6/','/07/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/7/','/08/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/8/','/09/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/9/','/10/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/10/','/11/'))
df_americas['Date'] = df_americas['Date'].apply(lambda x: x.replace('/12/','/12/'))
df_americas['Date'] = pandas.to_datetime(df_americas['Date'], errors='coerce').dt.strftime("%Y-%m-%d")
df_americas = df_americas.dropna()
df_americas.columns = ['DATE', 'COUNTRY', 'PRICE']

df_global = pandas.read_excel(f'{path}/Global_Swine_Prices_Weekly.xlsx')
df_global = df_global.drop(columns='SPAIN')
df_global = pandas.melt(df_global, id_vars=['Date'], var_name='COUNTRY', value_name='PRICE')
df_global['PRICE'] = df_global['PRICE'].round(2)

df_global['Date'] = pandas.to_datetime(df_global['Date'].astype(str).str.strip(), format = '%d/%m/%Y', errors='coerce') #################
df_global['Date'] = df_global['Date'].dt.strftime("%Y-%m-%d") ########

df_global = df_global.dropna()
df_global.columns = ['DATE', 'COUNTRY', 'PRICE']

df_asia = pandas.read_excel(f'{path}/Asian_Hogs_Prices.xlsx')
df_asia = pandas.melt(df_asia, id_vars=['Date'], var_name='COUNTRY', value_name='PRICE')
df_asia = df_asia.apply(lambda x: x.str.replace(',','.'))
df_asia['PRICE'] = pandas.to_numeric(df_asia['PRICE'])
df_asia['PRICE'] = df_asia['PRICE'].round(2)
df_asia['COUNTRY'] = df_asia['COUNTRY'].str.upper()
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/0/','/01/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/1/','/02/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/2/','/03/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/3/','/04/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/4/','/05/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/5/','/06/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/6/','/07/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/7/','/08/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/8/','/09/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/9/','/10/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/10/','/11/'))
df_asia['Date'] = df_asia['Date'].apply(lambda x: x.replace('/12/','/12/'))
df_asia['Date'] = pandas.to_datetime(df_asia['Date'], errors='coerce').dt.strftime("%Y-%m-%d")
df_asia = df_asia.dropna()
df_asia.columns = ['DATE', 'COUNTRY', 'PRICE']

df_final = pandas.concat([df_europa, df_americas], ignore_index=True)
df_final = pandas.concat([df_final, df_global], ignore_index=True)
df_final = pandas.concat([df_final, df_asia], ignore_index=True)




cursor = cnn.cursor()

x = cursor.execute('USE DATABASE DB_BR_RISKMANAGEMENT')
x = cursor.execute(f'USE SCHEMA MID')
x = cursor.execute('USE WAREHOUSE WH_BR_TEAMRISK_XS')

# df_final['DATE'] = pandas.to_datetime(df_final['DATE'], errors='coerce')
df_final['DATE'] = pandas.to_datetime(df_final['DATE']).dt.strftime("%Y-%m-%d")

x = 1
cursor.execute(f"DELETE FROM GLOBAL_HOG_PRICES")


x = spd.write_pandas(cnn, df_final, table_name="GLOBAL_HOG_PRICES")

print(f'Global Hog Prices updated')