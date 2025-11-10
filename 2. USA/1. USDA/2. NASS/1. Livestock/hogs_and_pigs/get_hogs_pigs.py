import pandas
import requests
import concurrent.futures
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def hogs_and_pigs(proxies: dict, api_key: str):
    urls = [
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS,%20BREEDING%20-%20INVENTORY',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS,%20MARKET,%20120%20TO%20179%20LBS%20-%20INVENTORY',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS,%20MARKET,%2050%20TO%20119%20LBS%20-%20INVENTORY',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS,%20MARKET,%20LT%2050%20LBS%20-%20INVENTORY',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS,%20SOWS%20-%20FARROWED,%20MEASURED%20IN%20HEAD',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS%20-%20PIG%20CROP,%20MEASURED%20IN%20HEAD',
        f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=SURVEY&short_desc=HOGS%20-%20LITTER%20RATE,%20MEASURED%20IN%20PIGS%20/%20LITTER'
    ]

    keys_to_keep = ['load_time', 'year', 'reference_period_desc', 'state_alpha', 'state_name', 'commodity_desc', 'class_desc', 'short_desc', 'prodn_practice_desc', 'domain_desc', 'domaincat_desc', 'agg_level_desc', 'freq_desc', 'Value', 'unit_desc']

    def fetch_data(url):
        response = requests.get(url, proxies=proxies)
        return response.json()['data']

    def process_data(data):
        processed_data = []
        for item in data:
            processed_data.append({key: item[key] for key in keys_to_keep})
        return processed_data

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(fetch_data, urls))

    all_data = []
    for result in results:
        all_data.extend(process_data(result))

    df_hog_pig = pandas.DataFrame(all_data)
    df_hog_pig = df_hog_pig[['load_time', 'year', 'reference_period_desc', 'commodity_desc', 'class_desc', 'short_desc', 'prodn_practice_desc', 'state_alpha', 'state_name', 'domain_desc', 'domaincat_desc', 'agg_level_desc', 'freq_desc', 'unit_desc', 'Value']]
    df_hog_pig['load_time'] = pandas.to_datetime(df_hog_pig['load_time']).dt.strftime('%m/%d/%Y')
    df_hog_pig['Value'] = df_hog_pig['Value'].astype(str).str.replace(',', '')
    df_hog_pig['Value'] = pandas.to_numeric(df_hog_pig['Value'], errors='coerce')

    df_hog_pig.columns = ['REPORT_DATE', 'YEAR', 'MONTH', 'COMMODITY_DESCRIPTION', 'CLASS_DESCRIPTION', 'SHORT_DESCRIPTION', 'PRATICE_DESCRIPTION', 'STATE', 'STATE_NAME', 'DOMAIN_DESCRIPTION', 'DOMAIN_CATEGORY', 'AGGREGATED_LEVEL', 'FREQUENCY_DESCRIPTION', 'UNIT_DESCRIPTION', 'VALUE']
    return df_hog_pig
