import sys
import os
import pandas
import snowflake.connector
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Cruz/Codes/Inteligência')
from snowflake_connector import snowflake_connector



def create_snowflake_connection():
    creds = snowflake_connector()
    connection = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        proxy_host=creds['proxy_host'],
        proxy_port=creds['proxy_port']
    )
    return connection


def set_snowflake_context(cursor, warehouse = 'WH_BR_TEAMRISK_XS', database = 'DB_BR_RISKMANAGEMENT', schema = 'MID'):
    """
    Define as variáveis do SnowFlake que serão utilizadas: Warehouse, Banco de dados e Schema
    """
    cursor.execute(f"USE WAREHOUSE {warehouse};")
    cursor.execute(f"USE DATABASE {database};")
    cursor.execute(f"USE SCHEMA {schema};")


def insert_if_new_data(conn, cursor, df, table_name, date_col_name_ref, key_column=None, key_value=None):
    """
    Insere os dados no Snowflake se ainda não estiverem presentes.
    """
    last_date = df[f'{date_col_name_ref}'].max()
    
    if key_column and key_value:
        query = f"SELECT MAX({date_col_name_ref}) FROM {table_name} WHERE {key_column} = '{key_value}'"
    else:
        query = f"SELECT MAX({date_col_name_ref}) FROM {table_name}"
    
    cursor.execute(query)
    result = cursor.fetchall()
    last_snowflake_date = result[0][0]

    cursor.execute(f"SELECT * FROM {table_name} WHERE {date_col_name_ref} = '{last_date}'")
    results = cursor.fetchall()

    if not results:
        df_to_insert = df[df[f'{date_col_name_ref}'] > str(last_snowflake_date)]
        df_to_insert[f'{date_col_name_ref}'] = pandas.to_datetime(df_to_insert[f'{date_col_name_ref}']).dt.strftime('%Y-%m-%d')
        spd.write_pandas(conn, df_to_insert, table_name=table_name)
        print(f'Data inserted into {table_name} for {key_value or "general"} at {last_date}')
    else:
        print(f'Data already exists in {table_name} for {key_value or "general"} at {last_date}')

def insert_if_new_data_by_country(conn, cursor, df, table_name, date_col_name_ref, country_col_name, product_col_name):
    """
    Insere os dados no Snowflake por país, se ainda não estiverem presentes.
    Considera a última data registrada no banco para cada país.
    """
    products = df[product_col_name].unique()
    countries = df[country_col_name].unique()

    for product in products:
        for country in countries:
            # Última data no dataframe para esse país
            last_date = df[df[country_col_name] == country][date_col_name_ref].max()
            print(f"Max data for {country} is {last_date}")

            # Consulta a última data já inserida no banco para esse país
            query = f"""
                SELECT MAX({date_col_name_ref}) 
                FROM {table_name} 
                WHERE {country_col_name} = '{country}'
            """
            cursor.execute(query)
            result = cursor.fetchall()
            last_snowflake_date = result[0][0]

            # Filtra os dados novos
            df_country = df[df[country_col_name] == country]

            if last_snowflake_date is None:
                # Nenhum dado ainda no banco para esse país — insere tudo
                df_to_insert = df_country
            else:
                # Apenas dados mais novos
                df_country[date_col_name_ref] = df_country[date_col_name_ref].dt.date
                df_to_insert = df_country[df_country[date_col_name_ref] > last_snowflake_date]

            if not df_to_insert.empty:
                df_to_insert[date_col_name_ref] = pandas.to_datetime(df_to_insert[date_col_name_ref]).dt.strftime('%Y-%m-%d')
                spd.write_pandas(conn, df_to_insert, table_name=table_name)
                print(f"Data inserted into {table_name} for {product} in {country} up to {last_date}")
            else:
                print(f"No new data to insert for {country} for {product}")