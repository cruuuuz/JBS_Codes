import sys
import pandas
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Codes')
from credentials import create_snowflake_connection, set_snowflake_context

def send_Progress(dict_dfs):

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    tables = {
        'Corn_Progress'          : 'US_CROP_CORN_PROGRESS',
        'Spring_Wheat_ex_durum'  : 'US_CROP_WHEAT_PROGRESS',
        'Spring_Wheat_Durum'     : 'US_CROP_WHEAT_PROGRESS',
        'Winter_Wheat'           : 'US_CROP_WHEAT_PROGRESS',
        'Soybean'                : 'US_CROP_SOYBEAN_PROGRESS'
    }

    for name, item in dict_dfs.items():
        df = item
        df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE']).dt.strftime('%Y-%m-%d')
        last_date = df['REPORT_DATE'].max()

        # Verifica se a data já está no banco
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