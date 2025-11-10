import requests, sys, json, os, pandas, time, datetime, asyncio, httpx
import snowflake.connector
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Codes')
from credentials import proxy_request, create_snowflake_connection, set_snowflake_context

#Define o path para armazenar os arquivos
path = 'O:/Cruz/Inteligencia/USA/USDA/FAS_API' #Pode colocar algum path do Jenkins
if not os.path.exists(path):
    path = os.path.dirname(os.path.abspath(__file__))

#Define o proxy para a requisição
proxies = proxy_request()

#Defini o API_KEY do FAS, obtido no site: https://apps.fas.usda.gov/opendataweb/home
api_key = {
    'API_KEY' : '4ea0d75b-c0b1-433e-a9ce-9ab5ac788c5e'
}

for arquivo in os.listdir(path):
    if arquivo.endswith('.json'):
        caminho_arquivo = os.path.join(path, arquivo)
        os.remove(caminho_arquivo)

#Funções base
def get_commodity_code():
    '''
    A função faz a requisição no API do FAS e retorna o json com todos os registros referentes aos código e demais dados para cada commodity acompanhada pelo órgão
    '''
    global commodity_code
    url = 'https://apps.fas.usda.gov/OpenData/api/esr/commodities'
    file_name = 'Commodity_Code.json'

    if os.path.exists(f'{path}/{file_name}'):
        with open(f'{path}/{file_name}', 'r') as f:
            commodity_code = json.loads(f.read())
            return print('Commodity_Code already exists')
    else:
        response = requests.get(url, proxies = proxies, headers=api_key)
        with open(f'{path}/{file_name}', 'wb') as f:
            f.write(response.content)
        with open(f'{path}/{file_name}', 'r') as j:
            commodity_code = json.loads(j.read())
            return print('Commodities codes downloaded')
def get_countries_code():
    '''
    A função faz a requisição no API do FAS e retorna o json com todos os registros referentes aos código e demais dados para cada país destino acompanhada pelo órgão
    '''
    global countries_code
    url = 'https://apps.fas.usda.gov/OpenData/api/esr/countries'
    file_name = 'Countries_Code.json'

    if os.path.exists(f'{path}/{file_name}'):
        with open(f'{path}/{file_name}', 'r') as f:
            countries_code = json.loads(f.read())
            return print('Countries_Code already exists')
    else:
        response = requests.get(url, proxies = proxies, headers=api_key)
        with open(f'{path}/{file_name}', 'wb') as f:
            f.write(response.content)
        with open(f'{path}/{file_name}', 'r') as j:
            countries_code = json.loads(j.read())
            return print('Countries codes downloaded')
def get_date_reports():
    '''
    A função faz a requisição no API do FAS e retorna o json com todas as datas do último report e armazena as datas do último relatório de cada ano armazenado
    '''
    global date_reports 

    url = 'https://apps.fas.usda.gov/OpenData/api/esr/datareleasedates'
    file_name = 'Report_Dates.json'

    if os.path.exists(f'{path}/{file_name}'):
        with open(f'{path}/{file_name}', 'r') as f:
            date_reports = json.loads(f.read())
            return print('Report_Dates already exists')
    else:
        response = requests.get(url, proxies = proxies, headers=api_key)
        with open(f'{path}/{file_name}', 'wb') as f:
            f.write(response.content)
        with open(f'{path}/{file_name}', 'r') as j:
            date_reports = json.loads(j.read())
            return print('Report_Dates downloaded')


#Funções base de histórico completo
def export_sales_all_countries_all_commodities():
    global export_sales_data, lst_df

    inicio = time.time()

    for cm in range(len(commodity_code)):
        commodity = commodity_code[cm]['commodityCode']
        commodity_name = commodity_code[cm].get('commodityName')


        lst_df = []

        for y in range(datetime.date.today().year-1, 2000, -1):
            file_name = f'All_{commodity_name}_Exports_{y}.json'
            url = f'https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/{commodity}/allCountries/marketYear/{y}'

            try:
                if os.path.exists(f'{path}/{file_name}'):
                    with open(f'{path}/{file_name}', 'r') as f:
                        export_sales_data = json.loads(f.read())
                else:
                    response = requests.get(url, proxies = proxies, headers=api_key)
                    with open(f'{path}/{file_name}', 'wb') as f:
                        f.write(response.content)
                        with open(f'{path}/{file_name}', 'r') as j:
                            export_sales_data = json.loads(j.read())

                df = pandas.DataFrame(export_sales_data)

                for i in range(0, len(df)):
                    country_code = df.iloc[i,1]
                    for c in countries_code:
                        if c.get('countryCode') == country_code:
                            country_code = c.get('countryName')
                            df.iloc[i,1] = country_code 
                    commodities_code = df.iloc[i,0]
                    for ct in commodity_code:
                        if ct.get('commodityCode') == commodities_code:
                            commodities_code = ct.get('commodityName')
                            df.iloc[i,0] = commodities_code
                    i += 1


                df['weekEndingDate'] = pandas.to_datetime(df['weekEndingDate'], errors='coerce').dt.strftime("%Y-%m-%d")
                df['REPORT_DATE'] = df['weekEndingDate']
                df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE'])
                delta = datetime.timedelta(days=1)
                df['REPORT_DATE'] = df['REPORT_DATE']+delta
                lst_df.append(df)
            except:
                pass
            y += 1

        try:
            df_final = pandas.concat(lst_df, ignore_index=True)


            valor_procurado = df_final['weekEndingDate'].max()

            last_update_report = pandas.DataFrame(df_final[df_final['weekEndingDate'] == valor_procurado])
            last_update_report['REPORT_DATE'] = date_reports[0]['releaseTimeStamp']

            
            df_final.columns = ['COMMODITY_CODE', 'COUNTRY_CODE', 'WEEKLY_EXPORTS', 'ACCUMULATED_EXPORTS', 'OUTSTADING_SALES', 'GROSS_NEW_SALES', 'CURRENT_MY_NET_SALES', 'CURRENT_MY_TOTAL_COMMITMENT', 'NEXT_MY_OUTSTANDING_SALES', 'NEXT_MY_NET_SALES', 'UNIT', 'WEEK_ENDING_DATE', 'REPORT_DATE']
            df_final['REPORT_DATE'] = pandas.to_datetime(df_final['REPORT_DATE'], errors='coerce').dt.strftime("%Y-%m-%d")

            if os.path.exists(f'{path}/FAS_{commodity_name}_Data_All_Years.xlsx'):
                pass
            else:
                df_final.to_excel(f'{path}/FAS_{commodity_name}_Data_All_Years.xlsx', index=False)
        except:
            pass

        cm += 1

    fim = time.time()
    tempo = (fim - inicio)/60
    print(f'A execução levou {tempo} minutos')
def export_sales_all_countries_beef():
    global export_sales_data, lst_df

    inicio = time.time()

    commodity_name = 'Fresh, Chilled, or Frozen Muscle Cuts of Beef'
    beef_position = len(commodity_code)-2

    beef_code = commodity_code[beef_position]['commodityCode']

    lst_df = []

    for y in range(datetime.date.today().year-1, 2000, -1):
        file_name = f'All_{commodity_name}_Exports_{y}.json'
        url = f'https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/{beef_code}/allCountries/marketYear/{y}'

        if os.path.exists(f'{path}/{file_name}'):
            with open(f'{path}/{file_name}', 'r') as f:
                export_sales_data = json.loads(f.read())
        else:
            response = requests.get(url, proxies = proxies, headers=api_key)
            with open(f'{path}/{file_name}', 'wb') as f:
                f.write(response.content)
                with open(f'{path}/{file_name}', 'r') as j:
                    export_sales_data = json.loads(j.read())

        df = pandas.DataFrame(export_sales_data)

        for i in range(0, len(df)):
            country_code = df.iloc[i,1]
            for c in countries_code:
                if c.get('countryCode') == country_code:
                    country_code = c.get('countryName')
                    df.iloc[i,1] = country_code 
            df.iloc[i,0] = commodity_name
            i += 1

        df['weekEndingDate'] = pandas.to_datetime(df['weekEndingDate'], errors='coerce').dt.strftime("%Y-%m-%d")
        df['REPORT_DATE'] = df['weekEndingDate']
        df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE'])
        delta = datetime.timedelta(days=1)
        df['REPORT_DATE'] = df['REPORT_DATE']+delta
        lst_df.append(df)
        # df.to_excel(f'{path}/FAS_{commodity_name}_Data_{y}.xlsx', index=False)
        y += 1

    df_final = pandas.concat(lst_df, ignore_index=True)


    valor_procurado = df_final['weekEndingDate'].max()

    last_update_report = pandas.DataFrame(df_final[df_final['weekEndingDate'] == valor_procurado])
    last_update_report['REPORT_DATE'] = date_reports[0]['releaseTimeStamp']

    
    df_final.columns = ['COMMODITY_CODE', 'COUNTRY_CODE', 'WEEKLY_EXPORTS', 'ACCUMULATED_EXPORTS', 'OUTSTADING_SALES', 'GROSS_NEW_SALES', 'CURRENT_MY_NET_SALES', 'CURRENT_MY_TOTAL_COMMITMENT', 'NEXT_MY_OUTSTANDING_SALES', 'NEXT_MY_NET_SALES', 'UNIT', 'WEEK_ENDING_DATE', 'REPORT_DATE']
    df_final['REPORT_DATE'] = pandas.to_datetime(df_final['REPORT_DATE'], errors='coerce').dt.strftime("%Y-%m-%d")
    df_final.to_excel(f'{path}/FAS_{commodity_name}_Data_All_Years.xlsx', index=False)

    fim = time.time()
    tempo = (fim - inicio)/60
    print(f'A execução levou {tempo} minutos')
def export_sales_all_countries_pork():
    global export_sales_data, lst_df

    inicio = time.time()

    pork_position = len(commodity_code)-1
    commodity_name = 'Fresh, Chilled, or Frozen Muscle Cuts of Pork'
    pork_code = commodity_code[pork_position]['commodityCode']

    lst_df = []

    for y in range(datetime.date.today().year-1, 2012, -1):
        file_name = f'All_{commodity_name}_Exports_{y}.json'
        url = f'https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/{pork_code}/allCountries/marketYear/{y}'

        if os.path.exists(f'{path}/{file_name}'):
            with open(f'{path}/{file_name}', 'r') as f:
                export_sales_data = json.loads(f.read())
        else:
            response = requests.get(url, proxies = proxies, headers=api_key)
            with open(f'{path}/{file_name}', 'wb') as f:
                f.write(response.content)
                with open(f'{path}/{file_name}', 'r') as j:
                    export_sales_data = json.loads(j.read())

        df = pandas.DataFrame(export_sales_data)

        for i in range(0, len(df)):
            country_code = df.iloc[i,1]
            for c in countries_code:
                if c.get('countryCode') == country_code:
                    country_code = c.get('countryName')
                    df.iloc[i,1] = country_code 
            df.iloc[i,0] = commodity_name
            i += 1

        df['weekEndingDate'] = pandas.to_datetime(df['weekEndingDate'], errors='coerce').dt.strftime("%Y-%m-%d")
        df['REPORT_DATE'] = df['weekEndingDate']
        df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE'])
        delta = datetime.timedelta(days=1)
        df['REPORT_DATE'] = df['REPORT_DATE']+delta
        lst_df.append(df)
        y += 1

    df_final = pandas.concat(lst_df, ignore_index=True)


    valor_procurado = df_final['weekEndingDate'].max()

    last_update_report = pandas.DataFrame(df_final[df_final['weekEndingDate'] == valor_procurado])
    last_update_report['REPORT_DATE'] = date_reports[0]['releaseTimeStamp']

    
    df_final.columns = ['COMMODITY_CODE', 'COUNTRY_CODE', 'WEEKLY_EXPORTS', 'ACCUMULATED_EXPORTS', 'OUTSTADING_SALES', 'GROSS_NEW_SALES', 'CURRENT_MY_NET_SALES', 'CURRENT_MY_TOTAL_COMMITMENT', 'NEXT_MY_OUTSTANDING_SALES', 'NEXT_MY_NET_SALES', 'UNIT', 'WEEK_ENDING_DATE', 'REPORT_DATE']
    df_final['REPORT_DATE'] = pandas.to_datetime(df_final['REPORT_DATE'], errors='coerce').dt.strftime("%Y-%m-%d")
    df_final.to_excel(f'{path}/FAS_{commodity_name}_Data_All_Years.xlsx', index=False)

    fim = time.time()
    tempo = (fim - inicio)/60
    print(f'A execução levou {tempo} minutos')
    return df_final


def export_sales_all_countries_all_commodities_last_year():
    global export_sales_data, lst_df

    inicio = time.time()
    
    all_commodity_lst = []

    for cm in range(len(commodity_code)):
        commodity = commodity_code[cm]['commodityCode']
        commodity_name = commodity_code[cm].get('commodityName')

        lst_df = []

        y = datetime.date.today().year
        file_name = f'All_{commodity_name}_Exports_{y}.json'
        url = f'https://apps.fas.usda.gov/OpenData/api/esr/exports/commodityCode/{commodity}/allCountries/marketYear/{y}'

        try:
            if os.path.exists(f'{path}/{file_name}'):
                with open(f'{path}/{file_name}', 'r') as f:
                    export_sales_data = json.loads(f.read())
            else:
                response = requests.get(url, proxies = proxies, headers=api_key)
                with open(f'{path}/{file_name}', 'wb') as f:
                    f.write(response.content)
                    with open(f'{path}/{file_name}', 'r') as j:
                        export_sales_data = json.loads(j.read())

            df = pandas.DataFrame(export_sales_data)

            for i in range(0, len(df)):
                country_code = df.iloc[i,1]
                for c in countries_code:
                    if c.get('countryCode') == country_code:
                        country_code = c.get('countryName')
                        df.iloc[i,1] = country_code 
                commodities_code = df.iloc[i,0]
                for ct in commodity_code:
                    if ct.get('commodityCode') == commodities_code:
                        commodities_code = ct.get('commodityName')
                        df.iloc[i,0] = commodities_code
                i += 1


            df['weekEndingDate'] = pandas.to_datetime(df['weekEndingDate'], errors='coerce').dt.strftime("%Y-%m-%d")
            df['REPORT_DATE'] = df['weekEndingDate']
            df['REPORT_DATE'] = pandas.to_datetime(df['REPORT_DATE'])
            delta = datetime.timedelta(days=1)
            df['REPORT_DATE'] = df['REPORT_DATE']+delta
            lst_df.append(df)
        except:
            pass


        try:
            df_final = pandas.concat(lst_df, ignore_index=True)

            valor_procurado = df_final['weekEndingDate'].max()

            last_update_report = pandas.DataFrame(df_final[df_final['weekEndingDate'] == valor_procurado])
            
            df_final.columns = ['COMMODITY_CODE', 'COUNTRY_CODE', 'WEEKLY_EXPORTS', 'ACCUMULATED_EXPORTS', 'OUTSTADING_SALES', 'GROSS_NEW_SALES', 'CURRENT_MY_NET_SALES', 'CURRENT_MY_TOTAL_COMMITMENT', 'NEXT_MY_OUTSTANDING_SALES', 'NEXT_MY_NET_SALES', 'UNIT', 'WEEK_ENDING_DATE', 'REPORT_DATE']
            df_final['REPORT_DATE'] = pandas.to_datetime(df_final['REPORT_DATE'], errors='coerce').dt.strftime("%Y-%m-%d")

            if os.path.exists(f'{path}/FAS_{commodity_name}_Data_All_Years.xlsx'):
                all_commodity_lst.append(df_final)
                pass
            else:
                all_commodity_lst.append(df_final)
                df_final.to_excel(f'{path}/FAS_{commodity_name}_Data_All_Years.xlsx', index=False)
        except:
            pass
        cm += 1

    fim = time.time()
    tempo = (fim - inicio)/60
    print(f'A execução levou {tempo} minutos')
    return all_commodity_lst


def send_to_snowflake():
    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    df_to_snowflake = export_sales_all_countries_all_commodities_last_year()
    df_to_snowflake = pandas.concat(df_to_snowflake, ignore_index=True)
    last_date = df_to_snowflake['WEEK_ENDING_DATE'].max()
    df_to_snowflake.columns = ['COMMODITY_NAME', 'COUNTRY_NAME', 'WEEKLY_EXPORTS', 'ACCUMULATED_EXPORTS', 'OUTSTADING_SALES', 'GROSS_NEW_SALES', 'CURRENT_MY_NET_SALES', 'CURRENT_MY_TOTAL_COMMITMENT', 'NEXT_MY_OUTSTADING_SALES', 'NEXT_MY_NET_SALES', 'UNIT', 'WEEK_ENDING_DATE', 'REPORT_DATE']

    consulta_last_date = cursor.execute(f"SELECT * FROM EXPORT_SALES WHERE WEEK_ENDING_DATE = '{last_date}'") 
    results = cursor.fetchall()
    if results == []:

        max_date_snowflake = cursor.execute(f"SELECT MAX(WEEK_ENDING_DATE) FROM EXPORT_SALES")
        result_date = cursor.fetchall()
        max_date_snowflake = str(result_date[0][0]) 

        df_to_snowflake = pandas.DataFrame(df_to_snowflake)
        df_send_to_snowflake = df_to_snowflake.loc[df_to_snowflake['WEEK_ENDING_DATE']>f'{max_date_snowflake}'] 
        x = spd.write_pandas(cnn, df_send_to_snowflake, table_name="EXPORT_SALES")
        for arquivo in os.listdir(path):
            if arquivo.endswith('.json'):
                caminho_arquivo = os.path.join(path, arquivo)
                os.remove(caminho_arquivo)
        return print(f'Data about {last_date} was updated')
    else:
        for arquivo in os.listdir(path):
            if arquivo.endswith('.json'):
                caminho_arquivo = os.path.join(path, arquivo)
                os.remove(caminho_arquivo)
        return print(f"Data about {last_date} already updated")



get_commodity_code()
get_countries_code()
get_date_reports()












