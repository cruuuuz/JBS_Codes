import pandas
import requests
import concurrent.futures
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def cold_storage(proxies: dict, api_key: str):
    urls = [
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=BEEF,%20BONE-IN,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=BEEF,%20BONELESS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=BEEF,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20BELLIES,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20BUTTS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20HAMS,%20BONE-IN,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20HAMS,%20BONELESS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20HAMS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20LOINS,%20BONE-IN,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20LOINS,%20BONELESS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20LOINS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20OTHER%20CLASS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20PICNICS,%20BONE-IN,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20RIBS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20TRIMMINGS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20UNCLASSIFIED,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=PORK,%20VARIETY%20MEATS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20BREASTS%20%26%20BREAST%20MEAT,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20DRUMSTICKS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20LEG%20QUARTERS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20LEGS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20MATURE,%20WHOLE,%20HENS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20OTHER%20PARTS%20%26%20FORMS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20PAWS%20%26%20FEET,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20THIGH%20%26%20THIGH%20QUARTERS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20THIGH%20MEAT,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20WINGS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=CHICKENS,%20YOUNG,%20WHOLE,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20BREASTS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20DEBONED%20MEAT,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20LEGS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20OTHER%20PARTS%20%26%20FORMS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20UNCLASSIFIED,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20WHOLE,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20WHOLE,%20HENS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=TURKEYS,%20WHOLE,%20TOMS,%20COLD%20STORAGE,%20FROZEN%20-%20STOCKS,%20MEASURED%20IN%20LB'
    ]

    keys_to_keep = ['load_time', 'year', 'reference_period_desc', 'sector_desc', 'group_desc', 'commodity_desc', 'short_desc', 'util_practice_desc', 'prodn_practice_desc', 'state_alpha', 'state_name', 'class_desc', 'statisticcat_desc', 'domain_desc', 'agg_level_desc', 'freq_desc', 'unit_desc', 'Value']

    def fetch_and_process(url):
        response = requests.get(url, proxies=proxies)
        response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
        conteudo_json = response.json()['data']
        
        # Utiliza list comprehension para criar uma lista de dicionários filtrados
        filtered_data = [{key: item[key] for key in keys_to_keep} for item in conteudo_json]
        
        df = pandas.DataFrame(filtered_data)
        
        # Otimiza a conversão de tipos de dados
        df['load_time'] = pandas.to_datetime(df['load_time']).dt.strftime("%d-%m-%Y")
        df['Value'] = df['Value'].astype(str).str.replace(',', '', regex=False)
        df['Value'] = pandas.to_numeric(df['Value'], errors='coerce')

        return df

    # Utiliza ThreadPoolExecutor com um número maior de workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_and_process, url) for url in urls]
        
        dfs_cold_storage = []
        for future in as_completed(futures):
            dfs_cold_storage.append(future.result())

    dfs_cold_storage = pandas.concat(dfs_cold_storage, ignore_index=True)
    dfs_cold_storage.columns = ['REPORT_DATE', 'YEAR', 'MONTH', 'SECTOR', 'GROUP_DESCRIPTION', 'COMMODITY_DESCRIPTION', 'SHORT_DESCRIPTION','UTIL_PRATICE_DESCRIPTION', 'PRATICE_DESCRIPTION', 'STATE', 'STATE_NAME', 'CLASS_DESCRIPTION', 'STATISTICAL_DESCRIPTION', 'DOMAIN_DESCRIPTION', 'AGGREGATED_LEVEL', 'FREQUENCY_DESCRIPTION', 'UNIT_DESCRIPTION', 'VALUE']

    return dfs_cold_storage