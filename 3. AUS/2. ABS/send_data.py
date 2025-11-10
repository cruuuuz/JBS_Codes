import sys
import pandas
sys.path.append('O:/Codes')

import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context

def send_Livestock(dict_dfs):
    """
    Envia dados de moagem de milho e coprodutos do etanol para Snowflake.
    
    Parâmetros:
    ----------
    dict_dfs : dict
        Dicionário com nome do conjunto (ex: 'Corn_usage') e seu respectivo DataFrame.

    Estrutura esperada:
        {
            'Corn_usage': DataFrame com dados de uso do milho,
            'Alcohol': DataFrame com dados de coprodutos do etanol
        }
    """

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    tables = {
        'Slaughter' : 'AUS_ABS_QUARTERLY_SLAUGHTER',
        'Production': 'AUS_ABS_QUARTERLY_PRODUCTION'
    }

    for name, df in dict_dfs.items():
        # Padroniza a data e o valor
        df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')

        # Evita erro: df_filtered ainda não foi definido aqui
        descriptions = df['PARAMETER'].drop_duplicates()

        for desc in descriptions:
            # Filtra os dados para uma commodity específica
            df_filtered = df[df['PARAMETER'] == desc]
            last_date = df_filtered['REPORT_DATE'].max()

            # Verifica se essa combinação de data + descrição já existe no banco
            query = f"""
                SELECT * FROM {tables[name]} 
                WHERE REPORT_DATE = '{last_date}' 
                AND PARAMETER = '{desc}'
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                # Busca a data mais recente presente no banco
                max_date_query = cursor.execute(
                    f"""
                    SELECT MAX(REPORT_DATE) 
                    FROM {tables[name]} 
                    WHERE PARAMETER = '{desc}'
                    """
                ).fetchall()
                max_date = max_date_query[0][0]

                # Filtra apenas os dados novos
                df_to_insert = df_filtered[df_filtered['REPORT_DATE'] > str(max_date)]

                if not df_to_insert.empty:
                    spd.write_pandas(cnn, df_to_insert, table_name=tables[name])
                    print(f"[✔] Data for {desc} ({last_date}) was inserted into {tables[name]}")
                else:
                    print(f"[ℹ] No new data to insert for {desc}")
            else:
                print(f"[✓] Data for {desc} ({last_date}) is already in {tables[name]}")
