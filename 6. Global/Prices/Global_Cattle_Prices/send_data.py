import pandas as pd
import snowflake.connector.pandas_tools as spd
import sys
sys.path.append('O:/Codes')
from credentials import create_snowflake_connection, set_snowflake_context


def send_prices(df):
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    table = 'GLOBAL_CATTLE_PRICES'

    df['REFERENCE_DATE'] = pd.to_datetime(df['REFERENCE_DATE']).dt.strftime('%Y-%m-%d')
    
    # spd.write_pandas(cnn, df, table_name='BRA_SECEX_MONTHLY_MEAT_EXPORTS')

    last_date = df['REFERENCE_DATE'].max()

    query = f"""
        SELECT * FROM {table} 
        WHERE REFERENCE_DATE = '{last_date}' 
    """
    cursor.execute(query)
    results = cursor.fetchall()

    if not results:
        # Busca a data mais recente presente no banco
        max_date_query = cursor.execute(
            f"""
            SELECT MAX(REFERENCE_DATE) 
            FROM {table} 
        """
        ).fetchall()
        max_date = max_date_query[0][0]

        # Filtra apenas os dados novos
        df_to_insert = df[df['REFERENCE_DATE'] > str(max_date)]

        if not df_to_insert.empty:
            spd.write_pandas(cnn, df_to_insert, table_name=table)
            print(f"[✔] Data for {last_date} was inserted into {table}")
        else:
            print(f"[ℹ] No new data to insert for global cattle prices")
    else:
        print(f"[✓] Data for {last_date} is already in {table}")


df_usd_cwt = pd.read_excel('O:/Cruz/Inteligencia/Acompanhamento Global de Preços/1_Cattle_Graphs.xlsm', sheet_name = 'Base_Price_USD_Graphs')
df_usd_cwt = df_usd_cwt.iloc[:, :10]

df_melted_usd_cwt = df_usd_cwt.melt(
    id_vars="Dates",
    var_name = "COUNTRY",
    value_name="PRICES"
    )

df_melted_usd_cwt['UNIT'] = 'USD/CWT'
df_melted_usd_cwt.columns = ['REFERENCE_DATE', 'COUNTRY', 'PRICES', 'UNIT']
df_melted_usd_cwt = df_melted_usd_cwt.dropna()
df_melted_usd_cwt['COUNTRY'] = df_melted_usd_cwt['COUNTRY'].str.upper()
df_melted_usd_cwt['PRICES'] = df_melted_usd_cwt['PRICES'].round(2)
send_prices(df_melted_usd_cwt)


df_usd_arroba = pd.read_excel('O:/Cruz/Inteligencia/Acompanhamento Global de Preços/1_Cattle_Graphs.xlsm', sheet_name = 'Base_Price_USD_@_Graphs')
df_usd_arroba = df_usd_arroba.iloc[:, :10]

df_melted_usd_arroba = df_usd_arroba.melt(
    id_vars="Dates",
    var_name = "COUNTRY",
    value_name="PRICES"
    )

df_melted_usd_arroba['UNIT'] = 'USD/CWT'
df_melted_usd_arroba.columns = ['REFERENCE_DATE', 'COUNTRY', 'PRICES', 'UNIT']
df_melted_usd_arroba = df_melted_usd_arroba.dropna()
df_melted_usd_arroba['COUNTRY'] = df_melted_usd_arroba['COUNTRY'].str.upper()
df_melted_usd_arroba['PRICES'] = df_melted_usd_arroba['PRICES'].round(2)
send_prices(df_melted_usd_arroba)