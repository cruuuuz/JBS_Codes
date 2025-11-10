import sys
import os
import pandas
from datetime import datetime, timedelta
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Codes')
from credentials import create_snowflake_connection, set_snowflake_context



def format_dates(df: pandas.DataFrame, cols=('WEEK_START_DATE', 'WEEK_ENDING_DATE')) -> pandas.DataFrame:
    for col in cols:
        df[col] = pandas.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
    return df


def insert_if_new_data(cursor, conn, df: pandas.DataFrame, table: str, product: str = None, date_col_name_ref: str = 'WEEK_ENDING_DATE'):
    """
    Insere dados no Snowflake apenas se a data mais recente ainda não existir.
    Se `product` for None, ignora a cláusula de filtro por produto.
    """
    last_date = df[f'{date_col_name_ref}'].max()

    if product:
        query_max = f"SELECT MAX({date_col_name_ref}) FROM {table} WHERE PRODUCT = '{product}'"
        query_exists = f"SELECT * FROM {table} WHERE {date_col_name_ref} = '{last_date}' AND PRODUCT = '{product}'"
    else:
        query_max = f"SELECT MAX({date_col_name_ref}) FROM {table}"
        query_exists = f"SELECT * FROM {table} WHERE {date_col_name_ref} = '{last_date}'"

    # Busca data mais recente
    cursor.execute(query_max)
    result_date = cursor.fetchall()
    date_snowflake = str(result_date[0][0])

    # Verifica se já existe
    cursor.execute(query_exists)
    results = cursor.fetchall()

    if results == []:
        df_to_insert = df[df[f'{date_col_name_ref}'] > date_snowflake].copy()
        df_to_insert = format_dates(df_to_insert)
        spd.write_pandas(conn, df_to_insert, table_name=table)
        print(f'Data about {product or table} at date {last_date} was updated')
    else:
        print(f'Data about {product or table} at date {last_date} already exists')


def insert_if_new_data_by_country(conn, cursor, df, table_name, date_col_name_ref, country_col_name, species_col_name, category_col_name):
    """
    Insere os dados no Snowflake por país, se ainda não estiverem presentes.
    Considera a última data registrada no banco para cada país.
    """
    products = df[species_col_name].unique()
    countries = df[country_col_name].unique()
    categories = df[category_col_name].unique()
    

    for product in products:
        for country in countries:
            for category in categories:
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
                    print(f"No new data to insert for {country} for {product} - {category}")



import pandas as pd

def upsert_if_modified_data_by_country(conn, cursor, df, table_name,
                                       date_col, country_col, species_col,
                                       category_col, value_col):
    """
    Compara dados por (REFERENCE_DATE, COUNTRY, CATEGORY) e insere ou atualiza
    apenas se houver valor diferente no banco.
    """
    # Cria chave lógica no dataframe
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.date
    df['LOGICAL_ID'] = df.apply(
        lambda row: f"{row[date_col]}|{row[country_col]}|{row[category_col]}",
        axis=1
    )
    df['VALOR'] = df[value_col].astype(float).round(6)

    # Pega chaves únicas do dataframe
    logical_ids = df['LOGICAL_ID'].unique().tolist()
    products = df[species_col].unique()
    countries = df[country_col].unique()
    categories = df[category_col].unique()

    # Consulta todos os dados existentes para essas chaves no banco
    placeholders = ', '.join(f"'{cid}'" for cid in countries)
    query = f"""
        SELECT {date_col}, {country_col}, {category_col}, {value_col}
        FROM {table_name}
        WHERE {country_col} IN ({placeholders})
    """
    cursor.execute(query)
    results = cursor.fetchall()
    colunas = [col[0] for col in cursor.description]
    df_existing = pd.DataFrame(results, columns=colunas)

    if df_existing.empty:
        print("Banco está vazio para esses países — inserindo tudo.")
        df_to_insert = df.copy()
    else:
        df_existing[date_col] = pd.to_datetime(df_existing[date_col]).dt.date
        df_existing['LOGICAL_ID'] = df_existing.apply(
            lambda row: f"{row[date_col]}|{row[country_col]}|{row[category_col]}",
            axis=1
        )
        df_existing['VALOR'] = df_existing[value_col].astype(float).round(6)

        # Conjunto de chaves + valor
        ids_existing = set(zip(df_existing['LOGICAL_ID'], df_existing['VALOR']))
        ids_new = set(zip(df['LOGICAL_ID'], df['VALOR']))

        ids_para_upsert = ids_new - ids_existing

        df_to_insert = df[df.apply(
            lambda row: (row['LOGICAL_ID'], row['VALOR']) in ids_para_upsert,
            axis=1
        )]

    if not df_to_insert.empty:
        df_to_insert[date_col] = pd.to_datetime(df_to_insert[date_col]).dt.strftime('%Y-%m-%d')
        spd.write_pandas(conn, df_to_insert.drop(columns=['LOGICAL_ID', 'VALOR']), table_name=table_name)
        print(f"Dados inseridos/atualizados na tabela {table_name} para {len(df_to_insert)} linhas.")
    else:
        print("Nenhum dado novo ou modificado para inserir.")





def send_prices(list_dfs: list):
    """
    Recebe uma lista de DataFrames contendo dados de preços e envia para o Snowflake.
    Faz o controle de atualização com base na data mais recente de cada produto.
    """
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    ref_date = (datetime.today() - timedelta(days=120)).strftime("%Y-%m-%d")

    # Beef & Pork
    df_beef_pork = pandas.concat([list_dfs[0], list_dfs[1]])
    df_beef_pork = pandas.concat([df_beef_pork, list_dfs[4]])
    for product in df_beef_pork['PRODUCT'].drop_duplicates():
        query = f"""
            DELETE FROM EUROPE_LIVESTOCK_PORK_BEEF_PRICES
            WHERE PRODUCT = '{product}'
            AND WEEK_ENDING_DATE > '{ref_date}'
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df_filtered = df_beef_pork[df_beef_pork['PRODUCT'] == product]
        df_filtered = format_dates(df_filtered)
        insert_if_new_data(
            conn=cnn,
            cursor=cursor,
            df=df_filtered,
            table="EUROPE_LIVESTOCK_PORK_BEEF_PRICES",
            date_col_name_ref="WEEK_ENDING_DATE",
            product=product
        )

    # Poultry
    df_poultry = pandas.DataFrame(list_dfs[2])
    for product in df_poultry['PRODUCT'].drop_duplicates():
        query = f"""
            DELETE FROM EUROPE_LIVESTOCK_POULTRY_PRICES
            WHERE PRODUCT = '{product}'
            AND WEEK_ENDING_DATE > '{ref_date}'
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df_filtered = df_poultry[df_poultry['PRODUCT'] == product]
        df_filtered = format_dates(df_filtered)
        insert_if_new_data(
            conn=cnn,
            cursor=cursor,
            df=df_filtered,
            table="EUROPE_LIVESTOCK_POULTRY_PRICES",
            date_col_name_ref="WEEK_ENDING_DATE",
            product=product
        )

    # Eggs
    df_eggs = pandas.DataFrame(list_dfs[3])
    for product in df_eggs['PRODUCT'].drop_duplicates():
        query = f"""
            DELETE FROM EUROPE_LIVESTOCK_EGG_PRICES
            WHERE PRODUCT = '{product}'
            AND WEEK_ENDING_DATE > '{ref_date}'
        """
        cursor.execute(query)
        results = cursor.fetchall()
        df_filtered = df_eggs[df_eggs['PRODUCT'] == product]
        df_filtered = format_dates(df_filtered)
        insert_if_new_data(
            conn=cnn,
            cursor=cursor,
            df=df_filtered,
            table="EUROPE_LIVESTOCK_EGG_PRICES",
            date_col_name_ref="WEEK_ENDING_DATE",
            product=product
        )



def send_production(list_dfs: list):
    """
    Recebe uma lista de DataFrames contendo dados de preços e envia para o Snowflake.
    Faz o controle de atualização com base na data mais recente de cada produto.
    """
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    # Beef & Pork
    df_geral = pandas.concat(list_dfs)
    for animal_type in df_geral['ANIMAL_TYPE'].drop_duplicates():
        df_filtered = df_geral[df_geral['ANIMAL_TYPE'] == animal_type]
        query = f"""
            DELETE FROM EUROPE_LIVESTOCK_POULTRY_PRODUCTION WHERE ANIMAL_TYPE = '{animal_type}' AND REFERENCE_DATE >= '2025-01-01'
        """
        cursor.execute(query)
        result = cursor.fetchall()
        print(f'{animal_type} deleted')
    

        df_to_insert = df_filtered
        df_to_insert = df_to_insert[df_to_insert['REFERENCE_DATE'] >= '2024-01-01']
        df_to_insert['REFERENCE_DATE'] = pandas.to_datetime(df_to_insert['REFERENCE_DATE']).dt.strftime('%Y-%m-%d')
        spd.write_pandas(cnn, df_to_insert, table_name='EUROPE_LIVESTOCK_POULTRY_PRODUCTION')

        print(f'{animal_type} production updated')