import sys
import pandas
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Codes')
from credentials import create_snowflake_connection, set_snowflake_context


def send_Harvest_Plant(dict_dfs):

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    tables = {
        'harvested'      : 'US_CROP_HARVESTED',
        'planted'        : 'US_CROP_PLANTED'
    }

    for name, products in dict_dfs.items():
        if name == 'harvested':
            for product_name, df in products.items():
                print(f'Doing {product_name}, harvested')
                df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
                df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE']).dt.strftime('%Y-%m-%d')
                commodity = df['SHORT_DESCRIPTION'].iloc[0]

                df['VALUE'] = df['VALUE'].str.replace(',','')
                df['VALUE'] = pandas.to_numeric(df['VALUE'])

                df_last_year = df[df['SHORT_DESCRIPTION'] == commodity]
                last_date = df_last_year['REPORT_DATE'].max()

                states = list(df['STATE'].drop_duplicates())

                for i in range(len(states)):
                    state = states[i]
                    consulta = cursor.execute(
                        f"SELECT * FROM {tables[name]} WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{commodity}' AND STATE = '{state}'"
                    )
                    results = cursor.fetchall()

                    if not results:
                        max_date_query = cursor.execute(
                            f"SELECT MAX(REPORT_DATE) FROM {tables[name]} WHERE SHORT_DESCRIPTION = '{commodity}' AND STATE = '{state}'"
                        ).fetchall()
                        max_date = max_date_query[0][0]

                        df_filtered = df[df['REPORT_DATE'] > str(max_date)]
                        df_filtered = df_filtered[df_filtered['SHORT_DESCRIPTION'] == commodity]
                        df_filtered = df_filtered[df_filtered['STATE'] == state]
                        spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                        print(f'Data about {last_date} {name} {product_name} {state} was updated')
                    else:
                        print(f'Data about {last_date} {name} {product_name} {state} already updated')
        
        else:
            for product_name, df in products.items():
                print(f'Doing {product_name}, planted')
                df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
                df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE']).dt.strftime('%Y-%m-%d')
                commodity = df['SHORT_DESCRIPTION'].iloc[0]

                df['VALUE'] = df['VALUE'].str.replace(',','')
                df['VALUE'] = pandas.to_numeric(df['VALUE'])

                df_last_year = df[df['SHORT_DESCRIPTION'] == commodity]
                last_date = df_last_year['REPORT_DATE'].max()

                states = list(df['STATE'].drop_duplicates())

                for i in range(len(states)):
                    state = states[i]
                    consulta = cursor.execute(
                        f"SELECT * FROM {tables[name]} WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{commodity}' AND STATE = '{state}'"
                    )
                    results = cursor.fetchall()

                    if not results:
                        max_date_query = cursor.execute(
                            f"SELECT MAX(REPORT_DATE) FROM {tables[name]} WHERE SHORT_DESCRIPTION = '{commodity}' AND STATE = '{state}'"
                        ).fetchall()
                        max_date = max_date_query[0][0]

                        df_filtered = df[df['REPORT_DATE'] > str(max_date)]
                        df_filtered = df_filtered[df_filtered['SHORT_DESCRIPTION'] == commodity]
                        df_filtered = df_filtered[df_filtered['STATE'] == state]
                        spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                        print(f'Data about {last_date} {name} {product_name} {state} was updated')
                    else:
                        print(f'Data about {last_date} {name} {product_name} {state} already updated')