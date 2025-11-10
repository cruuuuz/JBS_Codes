from datetime import datetime
import os
from collect.service import request_data
from send_data import send_Progress
from dotenv import load_dotenv
from credentials import proxy_request

load_dotenv()

YEAR = datetime.now().year-1
API_KEY = os.getenv('NASS_API_KEY')
PROXIES = proxy_request()

def main(proxies: dict, year: int, api_key: str):
    send_Progress(request_data(proxies, year, api_key)) 

if __name__ == "__main__":
    try:
        main(PROXIES, YEAR, API_KEY)
    except Exception as e:  
        print(f"Error in attempt: {e}")