import sys
from get_hogs_pigs import hogs_and_pigs
from send_data import send_hogs_pigs
from credentials import proxy_request

API_KEY = '7CDADE48-49F1-3595-9E20-4BFAE55AF652'

PROXIES = proxy_request()


def main(proxies: dict, api_key: str):
    data = hogs_and_pigs(proxies, api_key)
    send_hogs_pigs(data)

if __name__ == "__main__":
    try:
        main(PROXIES, API_KEY)
    except Exception as e:
        print(f"Error in attempt: {e}")