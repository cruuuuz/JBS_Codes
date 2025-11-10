import os
import sys
from datetime import datetime
from get_data.service import request_data
from send_data import send_Livestock_Production
sys.path.append('O:/Codes')
from credentials import proxy_request
from dotenv import load_dotenv

load_dotenv()

YEAR = datetime.now().year
API_KEY = os.getenv('NASS_API_KEY')

PROXIES = proxy_request()


def main(proxies: dict, year: int, api_key: str):
    data = request_data(proxies, year, api_key)
    send_Livestock_Production(data)

if __name__ == "__main__":
    try:
        main(PROXIES, YEAR, API_KEY)
    except Exception as e:
        print(f"Error in attempt: {e}")