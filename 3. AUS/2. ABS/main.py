from datetime import datetime
from collect import service
from send_data import send_Livestock
import sys
import os

sys.path.append('O:/Codes')
from credentials import proxy_request
from dotenv import load_dotenv

load_dotenv()

YEAR = datetime.now().year

PROXIES = proxy_request()

def main(proxies: dict, year: int):
    send_Livestock(service.request_data(proxies, year))

if __name__ == "__main__":
    try:
        main(PROXIES, YEAR)
    except Exception as e:
        print(f"Error in attempt: {e}") 