from urllib.parse import quote
from io import BytesIO
import pandas
import requests
from datetime import datetime




class ABSCollector:
    @staticmethod
    def collect_slaughter_production(proxies: dict, year: int) -> pandas.DataFrame:

        itens = [7215001, 7215009]

        ref_month = {
            1 : 'dez',
            2 : 'mar',
            3 : 'mar',
            4 : 'mar',
            5 : 'jun',
            6 : 'jun',
            7 : 'jun',
            8 : 'set',
            9 : 'set',
            10 : 'set',
            11 : 'dez',
            12 : 'dez',
        }

        mes_atual = datetime.today().month

        dfs = []

        for i in range(len(itens)):
            url = f'https://www.abs.gov.au/statistics/industry/agriculture/livestock-products-australia/{ref_month[7]}-{year}/{itens[i]}.xlsx' #só muda esse número da lista pelo número do último mês do semestre
            response = requests.get(url, proxies=proxies)

            if response.status_code == 200:
                df = pandas.read_excel(BytesIO(response.content), engine="openpyxl", sheet_name='Data1')
            else:
                print("Erro ao baixar o arquivo:", response.status_code)

            df = df.drop(0)
            for c in range(2,9):
                df = df.drop(c)

            series_type_row = df.iloc[0, 1:]  # ignora a primeira coluna (Data)
            df_data = df.iloc[1:].copy()      # remove a linha de "Series Type"

            colunas_parametros = df.columns[1:]
            df_data.columns = ['Data'] + list(colunas_parametros)

            df_melted = pandas.melt(df_data, id_vars=['Data'], var_name='parametro', value_name='Valores')

            mapa_series_type = dict(zip(colunas_parametros, series_type_row))
            df_melted['series_type'] = df_melted['parametro'].map(mapa_series_type)

            df_melted['Data'] = pandas.to_datetime(df_melted['Data'])
            df_melted['parametro'] = df_melted['parametro'].str.split(';').str[1].str.strip()

            df_melted = df_melted[['Data', 'parametro', 'series_type', 'Valores']]
            df_melted.columns = ['REPORT_DATE', 'PARAMETER', 'SERIES_TYPE', 'VALUE']
            df_melted = df_melted.sort_values(by='REPORT_DATE', ascending=False)

            dfs.append(df_melted)

        return dfs