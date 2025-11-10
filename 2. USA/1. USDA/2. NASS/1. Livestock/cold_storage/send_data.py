import sys
import pandas
sys.path.append('O:/Codes')
import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context



def send_Cold_Storage(df_send_to_snowflake):
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    itens = df_send_to_snowflake['SHORT_DESCRIPTION']
    itens = list(itens.drop_duplicates())

    for i in range(0,len(itens)):
        df_filtered = df_send_to_snowflake[df_send_to_snowflake['SHORT_DESCRIPTION'] == itens[i]]
        df_filtered['REPORT_DATE'] = pandas.to_datetime(df_filtered['REPORT_DATE']).dt.strftime('%Y-%m-%d')
        last_date = df_filtered['REPORT_DATE'].max()
        last_date_snowflake = cursor.execute(f"SELECT MAX(REPORT_DATE) FROM US_COLD_STORAGE WHERE SHORT_DESCRIPTION = '{itens[i]}'")
        result_date = cursor.fetchall()
        date_snowflake = str(result_date[0][0])
        consulta_last_date = cursor.execute(f"SELECT * FROM US_COLD_STORAGE WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{itens[i]}'") 
        results = cursor.fetchall()
        if results==[]:
            if date_snowflake == 'None' or date_snowflake == None:
                df_to_snowflake = df_filtered
            else:
                df_to_snowflake = df_filtered.loc[df_filtered['REPORT_DATE'] > date_snowflake]
            df_to_snowflake['REPORT_DATE'] = pandas.to_datetime(df_to_snowflake['REPORT_DATE']).dt.strftime('%m/%d/%Y')
            x = spd.write_pandas(cnn, df_to_snowflake, table_name="US_COLD_STORAGE")
            print(f'Data about {itens[i]} at date {last_date} was updated')
        else:
            print(f'Data about {itens[i]} at date {last_date} already exists')
