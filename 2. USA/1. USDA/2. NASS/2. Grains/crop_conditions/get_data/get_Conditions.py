import pandas
import requests
import concurrent.futures
from urllib.parse import quote
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def encode_url_param(text: str) -> str:
    return quote(text)

class Conditions:
    @staticmethod
    def Pasture_Condition_Data(proxies: dict, year: int, api_key: str):
        def fetch_data(condition):
            url = f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PASTURELAND%20-%20CONDITION,%20MEASURED%20IN%20PCT%20{condition}&year__GE={year}'
            response = requests.get(url, proxies=proxies)
            conteudo_json = response.json()['data']
            
            dfs = []
            for item in conteudo_json:
                keys_to_keep = ['Value', 'unit_desc', 'commodity_desc', 'short_desc', 'load_time', 'year', 'sector_desc', 'group_desc', 'week_ending', 'state_alpha', 'reference_period_desc', 'state_name']
                dict_filtrado = {key: item[key] for key in keys_to_keep}
                
                df = pandas.DataFrame(dict_filtrado, index=[0])
                df = df[['load_time', 'week_ending', 'reference_period_desc', 'year', 'sector_desc', 'group_desc', 'commodity_desc', 'short_desc', 'unit_desc', 'state_alpha', 'state_name', 'Value']]
                df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime('%m/%d/%Y')
                
                dfs.append(df)
            
            return pandas.concat(dfs, ignore_index=True)

        conditions = ['EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY POOR']
        
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(fetch_data, conditions))
        
        df_final = pandas.concat(dfs, ignore_index=True)
        df_final.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE']
        return df_final


    @staticmethod
    def Wheat_Condition_Data(proxies: dict, year: int, api_key: str):
        def fetch_data(wheat_type, condition):
            url = f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=WHEAT%2C%20{wheat_type}%20-%20CONDITION,%20MEASURED%20IN%20PCT%20{encode_url_param(condition)}&year__GE={year}'
            response = requests.get(url, proxies=proxies)
            conteudo_json = response.json()['data']
            
            dfs = []
            for item in conteudo_json:
                keys_to_keep = ['Value', 'unit_desc', 'commodity_desc', 'short_desc', 'load_time', 'year', 'sector_desc', 'group_desc', 'week_ending', 'state_alpha', 'reference_period_desc', 'state_name']
                dict_filtrado = {key: item[key] for key in keys_to_keep}
                
                df = pandas.DataFrame(dict_filtrado, index=[0])
                df = df[['load_time', 'week_ending', 'reference_period_desc', 'year', 'sector_desc', 'group_desc', 'commodity_desc', 'short_desc', 'unit_desc', 'state_alpha', 'state_name', 'Value']]
                df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime('%m/%d/%Y')
                
                dfs.append(df)
            
            return pandas.concat(dfs, ignore_index=True)

        wheat_types = ['WINTER', 'SPRING, DURUM', 'SPRING, (EXCL DURUM)']
        conditions = ['EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY POOR']

        
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(fetch_data, wheat_types, conditions))

        # Chamadas para as funções
        df_winter = dfs[0]
        df_winter['type'] = 'WINTER'
        df_spring_durum = dfs[1]
        df_spring_durum['type'] = 'SPRING DURUM'
        df_spring_ex_durum = dfs[2]
        df_spring_ex_durum['type'] = 'SPRING (EXCL DURUM)'

        df_winter.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE', 'TYPE']
        df_spring_durum.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE', 'TYPE']
        df_spring_ex_durum.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE', 'TYPE']

        return [df_winter, df_spring_durum, df_spring_ex_durum]

    @staticmethod
    def Corn_Condition_Data(proxies: dict, year: int, api_key: str):
        def fetch_data(condition):
            url = f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CORN%20-%20CONDITION,%20MEASURED%20IN%20PCT%20{condition}&year__GE={year}'
            response = requests.get(url, proxies=proxies)
            conteudo_json = response.json()['data']
            
            dfs = []
            for item in conteudo_json:
                keys_to_keep = ['Value', 'unit_desc', 'commodity_desc', 'short_desc', 'load_time', 'year', 'sector_desc', 'group_desc', 'week_ending', 'state_alpha', 'reference_period_desc', 'state_name']
                dict_filtrado = {key: item[key] for key in keys_to_keep}
                
                df = pandas.DataFrame(dict_filtrado, index=[0])
                df = df[['load_time', 'week_ending', 'reference_period_desc', 'year', 'sector_desc', 'group_desc', 'commodity_desc', 'short_desc', 'unit_desc', 'state_alpha', 'state_name', 'Value']]
                df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime('%m/%d/%Y')
                
                dfs.append(df)
            
            return pandas.concat(dfs, ignore_index=True)

        conditions = ['EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY POOR']
        
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(fetch_data, conditions))
        
        df_final = pandas.concat(dfs, ignore_index=True)
        df_final.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE']
        return df_final

    @staticmethod
    def Soybean_Condition_Data(proxies: dict, year: int, api_key: str):
        def fetch_data(condition):
            url = f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=SOYBEANS%20-%20CONDITION,%20MEASURED%20IN%20PCT%20{condition}&year__GE={year}'
            response = requests.get(url, proxies=proxies)
            conteudo_json = response.json()['data']
            
            dfs = []
            for item in conteudo_json:
                keys_to_keep = ['Value', 'unit_desc', 'commodity_desc', 'short_desc', 'load_time', 'year', 'sector_desc', 'group_desc', 'week_ending', 'state_alpha', 'reference_period_desc', 'state_name']
                dict_filtrado = {key: item[key] for key in keys_to_keep}
                
                df = pandas.DataFrame(dict_filtrado, index=[0])
                df = df[['load_time', 'week_ending', 'reference_period_desc', 'year', 'sector_desc', 'group_desc', 'commodity_desc', 'short_desc', 'unit_desc', 'state_alpha', 'state_name', 'Value']]
                df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime('%m/%d/%Y')
                
                dfs.append(df)
            
            return pandas.concat(dfs, ignore_index=True)

        conditions = ['EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY POOR']
        
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(fetch_data, conditions))
        
        df_final = pandas.concat(dfs, ignore_index=True)
        df_final.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE']
        return df_final
