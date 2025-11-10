import sys
import pandas
sys.path.append('O:/Codes')
import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context

def send_hogs_pigs(df_send_to_snowflake):
    
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')
    
    df_send_to_snowflake['REPORT_DATE'] = pandas.to_datetime(df_send_to_snowflake['REPORT_DATE']).dt.strftime('%Y-%m-%d')
    last_date = df_send_to_snowflake['REPORT_DATE'].max()
    last_date_snowflake = cursor.execute(f"SELECT MAX(REPORT_DATE) FROM US_HOGS_AND_PIGS")
    result_date = cursor.fetchall()
    date_snowflake = str(result_date[0][0])
    consulta_last_date = cursor.execute(f"SELECT * FROM US_HOGS_AND_PIGS WHERE REPORT_DATE = '{last_date}'") 
    results = cursor.fetchall()
    if results==[]:
        df_to_snowflake = df_send_to_snowflake.loc[df_send_to_snowflake['REPORT_DATE'] > date_snowflake]
        x = spd.write_pandas(cnn, df_to_snowflake, table_name="US_HOGS_AND_PIGS")
        print(f'Data about Hogs and Pigs at date {last_date} was updated')
    else:
        print(f'Data about Hogs and Pigs at date {last_date} already exists')
