import pandas as pd
import requests
import time
import concurrent.futures
from datetime import datetime
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor


class Slaughter:
    @staticmethod
    def Cattle_Slaughter_Data(proxies: dict, year: int, api_key: str) -> pd.DataFrame:

        def fetch_data(url: str):
            try:
                response = requests.get(url, proxies=proxies, timeout=30)
                response.raise_for_status()
                return response.json().get('data', [])
            except Exception as e:
                print(f"[Erro na URL] {url}\n{e}")
                return []

        def process_data(data):
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data)

            keys_to_keep = [
                'state_alpha', 'sector_desc', 'group_desc', 'class_desc',
                'year', 'state_name', 'reference_period_desc', 'unit_desc',
                'Value', 'load_time', 'short_desc', 'commodity_desc'
            ]

            df = df[[k for k in keys_to_keep if k in df.columns]].copy()

            df['load_time'] = pd.to_datetime(df['load_time'], errors='coerce').dt.strftime('%m/%d/%Y')
            df['Value'] = df['Value'].str.replace(',', '')
            df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

            return df

        short_descs = [
            "CATTLE, BULLS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, CALVES, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, COWS, MILK, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, COWS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, GE 500 LBS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, HEIFERS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, STEERS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, COWS, (EXCL MILK), SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "CATTLE, CALVES, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN HEAD",
        ]

        agg_levels = ['STATE', 'NATIONAL']

        urls = [
            f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&short_desc={quote(desc, safe=",")}&agg_level_desc={agg}&year_GE={year}'
            for agg in agg_levels for desc in short_descs
        ]

        # === Timer para diagnóstico ===
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as executor:
            fetched_data = list(executor.map(fetch_data, urls))
        print(f"Tempo de requisição total do Cattle Slaughter: {time.time() - t0:.2f} s")

        # Processamento em lote
        t1 = time.time()
        processed_dfs = [process_data(data) for data in fetched_data if data]
        df_final = pd.concat(processed_dfs, ignore_index=True)
        df_final = df_final[['load_time', 'year', 'reference_period_desc', 'sector_desc', 'group_desc', 'commodity_desc', 'class_desc', 'short_desc', 'state_alpha', 'state_name', 'unit_desc', 'Value']]
        print(f"Tempo de processamento dos dados: {time.time() - t1:.2f} s")

        # Filtro e renomeação
        valid_months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'YEAR']
        df_final = df_final[df_final['reference_period_desc'].isin(valid_months)]

        df_final.columns = [
            'REPORT_DATE', 'YEAR', 'MONTH', 'SECTOR', 'GROUP_DESCRIPTION',
            'COMMODITY_DESCRIPTION', 'CLASS_DESCRIPTION', 'SHORT_DESCRIPTION',
            'STATE', 'STATE_NAME', 'UNIT_DESCRIPTION', 'VALUE'
        ]

        return df_final

    
    @staticmethod
    def Hog_Slaughter_Data(proxies: dict, year: int, api_key: str) -> pd.DataFrame:

        def fetch_data(url: str):
            try:
                response = requests.get(url, proxies=proxies, timeout=30)
                response.raise_for_status()
                return response.json().get('data', [])
            except Exception as e:
                print(f"[Erro na URL] {url}\n{e}")
                return []

        def process_data(data):
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data)

            keys_to_keep = [
                'state_alpha', 'sector_desc', 'group_desc', 'class_desc',
                'year', 'state_name', 'reference_period_desc', 'unit_desc',
                'Value', 'load_time', 'short_desc', 'commodity_desc'
            ]

            df = df[[k for k in keys_to_keep if k in df.columns]].copy()

            df['load_time'] = pd.to_datetime(df['load_time'], errors='coerce').dt.strftime('%m/%d/%Y')
            df['Value'] = df['Value'].str.replace(',', '')
            df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

            return df

        short_descs = [
            "HOGS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN HEAD",
            "HOGS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "HOGS, BARROWS & GILTS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "HOGS, SOWS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD",
            "HOGS, BOARS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD"
        ]

        agg_levels = ['STATE', 'NATIONAL']

        urls = [
            f'https://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&short_desc={quote(desc, safe=",")}&agg_level_desc={agg}&year_GE={year}'
            for agg in agg_levels for desc in short_descs
        ]

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as executor:
            fetched_data = list(executor.map(fetch_data, urls))
        print(f"Tempo de requisição total do Hog Slaughter: {time.time() - t0:.2f} s")

        t1 = time.time()
        processed_dfs = [process_data(data) for data in fetched_data if data]
        df_final = pd.concat(processed_dfs, ignore_index=True)
        df_final = df_final[['load_time', 'year', 'reference_period_desc', 'sector_desc', 'group_desc', 'commodity_desc', 'class_desc', 'short_desc', 'state_alpha', 'state_name', 'unit_desc', 'Value']]
        print(f"Tempo de processamento dos dados: {time.time() - t1:.2f} s")

        valid_months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'YEAR']
        df_final = df_final[df_final['reference_period_desc'].isin(valid_months)]

        df_final.columns = [
            'REPORT_DATE', 'YEAR', 'MONTH', 'SECTOR', 'GROUP_DESCRIPTION',
            'COMMODITY_DESCRIPTION', 'CLASS_DESCRIPTION', 'SHORT_DESCRIPTION',
            'STATE', 'STATE_NAME', 'UNIT_DESCRIPTION', 'VALUE'
        ]

        return df_final
