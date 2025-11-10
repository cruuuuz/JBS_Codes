import requests 
import pandas

class Pigmeat:
    @staticmethod
    def get_Prices(base_url: str, proxies: dict, date: str):
        endpoint = "/api/pigmeat/prices?"
        params = {
            "memberStateCode" : "true",
            "beginDate"  : "01/01/2017",
            "endDate"    : date,
            "unit"       : "100 KG"
        }

        response = requests.get(f"{base_url}{endpoint}", params=params, proxies=proxies)
        
        df_geral = pandas.DataFrame(response.json())

        df_geral = df_geral[['endDate', 'beginDate', 'weekNumber', 'memberStateCode', 'memberStateName', 'pigClass', 'price', 'unit']]
        df_geral.columns = ['WEEK_ENDING_DATE', 'WEEK_START_DATE', 'WEEK_NUMBER', 'COUNTRY_CODE', 'COUNTRY_NAME', 'CATEGORY_OR_CLASSIFICATION', 'PRICE', 'UNIT']

        df_geral['PRICE'] = df_geral['PRICE'].str.replace('€','')
        df_geral['PRICE'] = pandas.to_numeric(df_geral['PRICE'])

        df_geral['WEEK_ENDING_DATE'] = pandas.to_datetime(df_geral['WEEK_ENDING_DATE'], format='%d/%m/%Y')
        df_geral['WEEK_START_DATE'] = pandas.to_datetime(df_geral['WEEK_START_DATE'], format='%d/%m/%Y')

        df_geral['COUNTRY_NAME'] = df_geral['COUNTRY_NAME'].str.upper()
        df_geral['CATEGORY_OR_CLASSIFICATION'] = df_geral['CATEGORY_OR_CLASSIFICATION'].str.upper()
        df_geral['UNIT'] = df_geral['UNIT'].str.upper()
        df_geral.insert(5, "PRODUCT", "PIGMEAT")

        return df_geral
