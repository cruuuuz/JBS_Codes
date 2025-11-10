from get_data.service import Weather_Data
from send_data import send_weather_data
import sys
from pathlib import Path

BASE_DIR_PATH = 'O:/Cruz'
INTELLIGENCE_PATH = Path(f'{BASE_DIR_PATH}/Codes/Inteligência')                                                                   

sys.path.append(str(INTELLIGENCE_PATH))
from password import password

PROXIES = {
        'http' : f'http://{password()}@MTZSVMFCPPRD02:8080',
        'https' : f'http://{password()}@MTZSVMFCPPRD02:8080'
    }


def main(proxies: dict):
    data = Weather_Data.get_weather_data(proxies)
    send_weather_data(data['prcp'], 'prcp')
    send_weather_data(data['tmin'], 'tmin')
    send_weather_data(data['tmax'], 'tmax')
    send_weather_data(data['tavg'], 'tavg')


if __name__ == "__main__":
    try:
        main(PROXIES)
    except Exception as e:
        print(f"Error: {e}")