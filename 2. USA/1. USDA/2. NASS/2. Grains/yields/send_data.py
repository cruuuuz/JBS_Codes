import sys
import pandas
sys.path.append('O:/Codes')
import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context

def send_Yields(dict_dfs):

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    for name, item in dict_dfs.items():
        df = item
        df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
        df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE']).dt.strftime('%Y-%m-%d')
        commodity = df['SHORT_DESCRIPTION'].iloc[0]

        df_last_year = df[df['SHORT_DESCRIPTION'] == commodity]
        last_date = df_last_year['REPORT_DATE'].max()

        # Verifica se a data já está no banco
        states = list(df['STATE'].drop_duplicates())

        for i in range(len(states)):
            state = states[i]
            consulta = cursor.execute(
                f"SELECT * FROM US_CROP_YIELDS WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{commodity}' AND STATE = '{state}'"
            )
            results = cursor.fetchall()

            if not results:
                max_date_query = cursor.execute(
                    f"SELECT MAX(REPORT_DATE) FROM US_CROP_YIELDS WHERE SHORT_DESCRIPTION = '{commodity}' AND STATE = '{state}'"
                ).fetchall()
                max_date = max_date_query[0][0]

                df_filtered = df[df['REPORT_DATE'] > str(max_date)]
                df_filtered = df_filtered[df_filtered['SHORT_DESCRIPTION'] == commodity]
                df_filtered = df_filtered[df_filtered['STATE'] == state]
                spd.write_pandas(cnn, df_filtered, table_name='US_CROP_YIELDS')
                print(f'Data about {last_date} {name} {state} Conditions was updated')
            else:
                print(f'Data about {last_date} {name} {state} Conditions already updated')