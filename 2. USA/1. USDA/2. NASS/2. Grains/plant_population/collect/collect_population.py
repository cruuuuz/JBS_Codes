from config.commodities import INDICATORS
from urllib.parse import quote
import pandas
import requests

def encode_url_param(text: str) -> str:
    return quote(text)

class PlantPopCollector:
    @staticmethod
    def collect_plant_population(commodity: str, proxies: dict, year: int, api_key: str) -> pandas.DataFrame:
        if commodity not in INDICATORS:
            raise ValueError(f"Commodity '{commodity}' não encontrada em INDICATORS")

        df_indicators = []

        for name, desc in INDICATORS[commodity].items():
            url = (
                f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}'
                f'&source_desc=SURVEY&short_desc={encode_url_param(desc)}&agg_level_desc=STATE&year__GE={year}'
            )
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                print(f"Erro na requisição para '{desc}': {response.status_code}")
                continue

            conteudo_json = response.json().get('data', [])
            dfs = []

            for item in conteudo_json:
                keys_to_keep = [
                    'load_time', 'week_ending', 'reference_period_desc', 'year', 'sector_desc', 
                    'group_desc', 'commodity_desc', 'short_desc', 'unit_desc', 'state_alpha',
                     'state_name', 'Value'
                ]
                dict_filtrado = {key: item[key] for key in keys_to_keep if key in item}
                df = pandas.DataFrame([dict_filtrado])
                df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime('%m/%d/%Y')
                dfs.append(df)

            if dfs:
                df_indicators.append(pandas.concat(dfs, ignore_index=True))

        for name, desc in INDICATORS[commodity].items():
            url = (
                f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}'
                f'&source_desc=SURVEY&short_desc={encode_url_param(desc)}&agg_level_desc=NATIONAL&year__GE={year}'
            )
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                print(f"Erro na requisição para '{desc}': {response.status_code}")
                continue

            conteudo_json = response.json().get('data', [])
            dfs = []

            for item in conteudo_json:
                keys_to_keep = [
                    'load_time', 'week_ending', 'reference_period_desc', 'year', 'sector_desc', 
                    'group_desc', 'commodity_desc', 'short_desc', 'unit_desc', 'state_alpha',
                     'state_name', 'Value'
                ]
                dict_filtrado = {key: item[key] for key in keys_to_keep if key in item}
                df = pandas.DataFrame([dict_filtrado])
                df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime('%m/%d/%Y')
                dfs.append(df)

            if dfs:
                df_indicators.append(pandas.concat(dfs, ignore_index=True))


        if df_indicators:
            df_indicators = pandas.concat(df_indicators, ignore_index=True)
            df_indicators.columns = ['REPORT_DATE', 'WEEK_ENDING_DATE', 'REFERENCE_WEEK', 'YEAR', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION', 'GROUP_CONDITION', 'STATE', 'STATE_NAME', 'VALUE']
            return df_indicators
        else:
            return pandas.DataFrame()  # ou talvez lançar erro dependendo do seu uso
