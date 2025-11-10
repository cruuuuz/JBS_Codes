import sys
import pandas
sys.path.append('O:/Codes')
import snowflake.connector.pandas_tools as spd
from credentials import create_snowflake_connection, set_snowflake_context



def send_Conditions(dict_dfs):

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    tables = {
        'Pasture_Conditions' : 'US_PASTURE_CONDITIONS',
        'Corn_Conditions'    : 'US_CORN_CONDITIONS',
        'Wheat_Conditions'   : 'US_WHEAT_CONDITIONS',
        'Soybean_Conditions' : 'US_SOYBEAN_CONDITIONS'
    }

    for name, item in dict_dfs.items():
        
        if name == 'Wheat_Conditions':
            for df in item:
                last_date = pandas.to_datetime(df['WEEK_ENDING_DATE']).max().date()
                wheat_type = df['TYPE'].max()

                consulta = cursor.execute(
                    f"SELECT MAX(WEEK_ENDING_DATE) FROM {tables[name]} WHERE TYPE = '{wheat_type}'"
                )
                try:
                    max_date = consulta.fetchall()[0][0]

                    if max_date < last_date:
                        df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE'])
                        df_filtered = df[df['WEEK_ENDING_DATE'] > pandas.to_datetime(max_date)]
                        df_filtered['REPORT_DATE'] = pandas.to_datetime(df_filtered['REPORT_DATE']).dt.strftime('%Y-%m-%d')
                        spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                        print(f'Data about {last_date} for {wheat_type} Wheat was updated')
                    else:
                        print(f'Data about {last_date} for {wheat_type} Wheat already updated')
                except:
                    max_date = pandas.to_datetime(max_date).date()

                    if max_date < last_date:
                        df['WEEK_ENDING_DATE'] = pandas.to_datetime(df['WEEK_ENDING_DATE'])
                        df_filtered = df[df['WEEK_ENDING_DATE'] > pandas.to_datetime(max_date)]
                        df_filtered['REPORT_DATE'] = pandas.to_datetime(df_filtered['REPORT_DATE']).dt.strftime('%Y-%m-%d')
                        df_filtered['WEEK_ENDING_DATE'] = pandas.to_datetime(df_filtered['WEEK_ENDING_DATE']).dt.strftime('%Y-%m-%d')
                        spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                        print(f'Data about {last_date} for {wheat_type} Wheat was updated')
                    else:
                        print(f'Data about {last_date} for {wheat_type} Wheat already updated')
        else:
            df = item
            df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
            last_date = df['REPORT_DATE'].max()

            consulta = cursor.execute(
                f"SELECT * FROM {tables[name]} WHERE REPORT_DATE = '{last_date}'"
            )
            results = cursor.fetchall()

            if not results:
                max_date_query = cursor.execute(
                    f"SELECT MAX(REPORT_DATE) FROM {tables[name]}"
                ).fetchall()
                max_date = max_date_query[0][0]

                df_filtered = df[df['REPORT_DATE'] > str(max_date)]
                spd.write_pandas(cnn, df_filtered, table_name=tables[name])
                print(f'Data about {last_date} {name} Conditions was updated')
            else:
                print(f'Data about {last_date} {name} Conditions already updated')
