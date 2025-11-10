import sys
import pandas
sys.path.append('O:/Codes')
import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context


def send_Livestock_Production(df_send_to_snowflake):
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    itens = list(df_send_to_snowflake.keys())

    snowflake_tables = {
        itens[0]  : 'US_LIVESTOCK_SLAUGHTER',
        itens[1]  : 'US_LIVESTOCK_SLAUGHTER',
        itens[2]  : 'US_LIVESTOCK_PRODUCTION',
        itens[3]  : 'US_LIVESTOCK_PRODUCTION',
        itens[4]  : 'US_LIVESTOCK_WEIGHTS',
        itens[5]  : 'US_LIVESTOCK_WEIGHTS'
    }

    for i in range(0,len(itens)):
        df_filtered = df_send_to_snowflake[itens[i]]
        df_filtered['REPORT_DATE'] = pandas.to_datetime(df_filtered['REPORT_DATE']).dt.strftime('%Y-%m-%d')
        last_date = df_filtered['REPORT_DATE'].max()

        desc = list(df_filtered['SHORT_DESCRIPTION'].drop_duplicates())
        for c in range(len(desc)):
            last_date_snowflake = cursor.execute(f"SELECT MAX(REPORT_DATE) FROM {snowflake_tables[itens[i]]} WHERE SHORT_DESCRIPTION = '{desc[c]}'")
            result_date = cursor.fetchall()
            date_snowflake = str(result_date[0][0])
            consulta_last_date = cursor.execute(f"SELECT * FROM {snowflake_tables[itens[i]]} WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{desc[c]}'") 
            results = cursor.fetchall()
            if results==[]:
                if date_snowflake == 'None' or date_snowflake == None:
                    df_to_snowflake = df_filtered
                else:
                    df_to_snowflake = df_filtered.loc[df_filtered['REPORT_DATE'] > date_snowflake]
                    df_to_snowflake = df_filtered.loc[df_filtered['SHORT_DESCRIPTION'] == desc[c]]
                df_to_snowflake['REPORT_DATE'] = pandas.to_datetime(df_to_snowflake['REPORT_DATE']).dt.strftime('%m/%d/%Y')
                x = spd.write_pandas(cnn, df_to_snowflake, table_name=f"{snowflake_tables[itens[i]]}")
                print(f'Data about {itens[i]} at date {last_date} was updated')
            else:
                print(f'Data about {itens[i]} at date {last_date} already exists')
