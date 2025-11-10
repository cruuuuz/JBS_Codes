import pandas
import sys
from prices.collect import get_all_prices


def get_Livestock_Prices(base_url: str, proxies: dict, date: str):
    return get_all_prices(base_url, proxies, date)