import pandas
import sys
from production.collect import get_all_production
sys.path.append('O:/Cruz/Codes/Inteligência')
from password import password


def get_Livestock_Production(base_url: str, proxies: dict, date: str):
    return get_all_production(base_url, proxies, date)