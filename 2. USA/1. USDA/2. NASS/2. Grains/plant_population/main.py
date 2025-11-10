from datetime import datetime
from collect.service import request_data
from send_data import send_Plant_Pop 
import os
from credentials import proxy_request
from dotenv import load_dotenv

load_dotenv()

YEAR = datetime.now().year
API_KEY = os.getenv('NASS_API_KEY')

PROXIES = proxy_request()

def main(proxies: dict, year: int, api_key: str):
    send_Plant_Pop(request_data(proxies, year, api_key)) 

if __name__ == "__main__":
    try:
        main(PROXIES, YEAR-1, API_KEY)
    except Exception as e:
        print(f"Error in attempt: {e}") 