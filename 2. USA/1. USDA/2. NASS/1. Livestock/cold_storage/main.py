from datetime import datetime
from pathlib import Path
from get_cold_storage import cold_storage 
from send_data import send_Cold_Storage
from credentials import proxy_request
import sys

API_KEY = '7CDADE48-49F1-3595-9E20-4BFAE55AF652'

PROXIES = proxy_request()


def main(proxies: dict, api_key: str):
    data = cold_storage(proxies, api_key)
    send_Cold_Storage(data)


if __name__ == "__main__":
    try:
        main(PROXIES, API_KEY)
    except Exception as e:
        print(f"Error in attempt: {e}")
    