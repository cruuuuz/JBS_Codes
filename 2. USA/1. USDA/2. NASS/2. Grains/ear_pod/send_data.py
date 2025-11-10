import sys
import pandas
sys.path.append('O:/Codes')
import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context

def send_EarsPods(dict_dfs):

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    tables = {
        'Corn'                : 'US_CROP_PLANT_POPULATION',
        'Soybean'             : 'US_CROP_PLANT_POPULATION'
    }

    for name, df in dict_dfs.items():
        if name == 'Corn':
            print(f'Doing {name}, ears')
            df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
            df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE']).dt.strftime('%Y-%m-%d')
            commodity = df['SHORT_DESCRIPTION'].iloc[0]

            df['VALUE'] = df['VALUE'].str.replace(',','')
            df['VALUE'] = pandas.to_numeric(df['VALUE'])

            df_last_year = df[df['SHORT_DESCRIPTION'] == commodity]
            last_date = df_last_year['REPORT_DATE'].max()

            consulta = cursor.execute(
                f"SELECT * FROM {tables[name]} WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{commodity}'"
            )
            results = cursor.fetchall()


            if not results:
                max_date_query = cursor.execute(
                    f"SELECT MAX(REPORT_DATE) FROM {tables[name]} WHERE SHORT_DESCRIPTION = '{commodity}'"
                ).fetchall()
                max_date = max_date_query[0][0]

                df_filtered = df[df['REPORT_DATE'] > str(max_date)]
                df_filtered = df_filtered[df_filtered['SHORT_DESCRIPTION'] == commodity]
                spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                print(f'Data about {last_date} {name} was updated')
            else:
                print(f'Data about {last_date} {name} already updated')
        else:
            print(f'Doing {name}, pods')
            df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
            df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE']).dt.strftime('%Y-%m-%d')
            commodity = df['SHORT_DESCRIPTION'].iloc[0]

            df['VALUE'] = df['VALUE'].str.replace(',','')
            df['VALUE'] = pandas.to_numeric(df['VALUE'], errors='coerce')

            df_last_year = df[df['SHORT_DESCRIPTION'] == commodity]
            last_date = df_last_year['REPORT_DATE'].max()

            consulta = cursor.execute(
                f"SELECT * FROM {tables[name]} WHERE REPORT_DATE = '{last_date}' AND SHORT_DESCRIPTION = '{commodity}'"
            )
            results = cursor.fetchall()



            if not results:
                max_date_query = cursor.execute(
                    f"SELECT MAX(REPORT_DATE) FROM {tables[name]} WHERE SHORT_DESCRIPTION = '{commodity}'"
                ).fetchall()
                max_date = max_date_query[0][0]

                df_filtered = df[df['REPORT_DATE'] > str(max_date)]
                df_filtered = df_filtered[df_filtered['SHORT_DESCRIPTION'] == commodity]
                spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                print(f'Data about {last_date} {name} {product_name} was updated')
            else:
                print(f'Data about {last_date} {name} {product_name} already updated')