from dates import Dates
import sys
from pathlib import Path
from base_files.service import modify_files
from cattle_prices.service import get_Global_Cattle_Prices
from organize_data.service import organize_data
from convert_data.service import convert_data

BASE_DIR_PATH = 'O:/Cruz'
CATTLE_PRICES_DIR = Path(f'{BASE_DIR_PATH}/Inteligencia/Acompanhamento Global de Preços/Cattle_Prices_Teste')
CATTLE_PRICES_DIR = Path(f'{BASE_DIR_PATH}/Inteligencia/Acompanhamento Global de Preços/Cattle_Prices')
INTELLIGENCE_PATH = Path(f'{BASE_DIR_PATH}/Codes/Inteligência')                                                                   
AMS_INTELLIGENCE_PATH = Path(f'{BASE_DIR_PATH}/Codes/Inteligência/USA/AMS_API')                                                                   

BASE_FILE_NAME = "Base.xlsm"
CURRENCY_FILE_NAME = 'Base_Currency.xlsm'

sys.path.append('O:/Codes')
from credentials import proxy_request


PROXIES = proxy_request()


ALL_DATES = Dates.dates()


def main(path: str, path_ams: str, proxies: dict, all_dates: dict, base_dir_path: str, base_file_name: str, currency_file_name: str):
    modify_files(path, base_dir_path, base_file_name, currency_file_name)                 
    get_Global_Cattle_Prices(path, path_ams, proxies, all_dates)                          
    organize_data(path, all_dates)
    convert_data(path)

if __name__ == "__main__":
    attempt = 0
    while attempt < 2:
        try:
            main(CATTLE_PRICES_DIR, AMS_INTELLIGENCE_PATH, PROXIES, ALL_DATES, BASE_DIR_PATH, BASE_FILE_NAME, CURRENCY_FILE_NAME)
            break
        except Exception as e:
            print(f"Error in attempt {attempt + 1}: {e}")
            attempt += 1
if attempt == 2:
    raise Exception("A função falhou após duas tentativas.")    