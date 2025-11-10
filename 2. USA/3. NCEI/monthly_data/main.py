import datetime
import sys
import pandas
from variables import States, Temps, Degrees, Palmers_Indx
from pathlib import Path

from get_data.temperatures import Temperatures
from get_data.precipitation import Precipitation
from get_data.degree_days import Degree_Days
from get_data.palmer_indexes import Palmer_Indexes
from send_data import send_weather_data

BASE_DIR_PATH = 'O:/Cruz'
INTELLIGENCE_PATH = Path(f'{BASE_DIR_PATH}/Codes/Inteligência')

sys.path.append(str(INTELLIGENCE_PATH))
from password import password

try:
    sys.path.append(str(INTELLIGENCE_PATH))
    from password import password
    PROXIES = {
            'http' : f'http://{password()}@MTZSVMFCPPRD02:8080',
            'https' : f'http://{password()}@MTZSVMFCPPRD02:8080'
        }
except:
    PROXIES = None

states = States.states()

temp_parameters = Temps.temp_parameters()

degree_measures = Degrees.degree_measures()

palmer_indx = Palmers_Indx.palmer_indx()

first_year = 1895
last_year = datetime.datetime.now().year

 
def main(states: dict, temp_parameters: list, degree_measures:list, palmer_indx: list, first_year: int, last_year: int, proxies: dict = PROXIES):
    temp = Temperatures.get_historical_temperatures(states, temp_parameters, first_year, last_year, proxies)
    precip = Precipitation.get_historical_precipitation(states, first_year, last_year, proxies)
    palmer = Palmer_Indexes.get_historical_palmer(states, palmer_indx, first_year, last_year, proxies)
    df_final = pandas.concat([temp, precip, palmer])
    df_final.columns = ['DATE', 'STATE', 'PARAMETER', 'UNIT', 'TIME_SCALE', 'VALUE']
    df_final['VALUE'] = pandas.to_numeric(df_final['VALUE'])
    df_final['DATE'] = pandas.to_datetime(df_final['DATE'], errors='coerce').dt.strftime("%Y-%m-%d")
    send_weather_data(df_final)


if __name__ == "__main__":
    try:
        main(states, temp_parameters, degree_measures, palmer_indx, first_year, last_year, PROXIES)
    except Exception as e:
        print(f"Error in attempt: {e}")
