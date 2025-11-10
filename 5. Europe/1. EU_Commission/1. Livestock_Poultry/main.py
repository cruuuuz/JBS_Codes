import sys
from prices.service import get_Livestock_Prices
from production.service import get_Livestock_Production
from inventory.service import get_Livestock_Inventory, send_inventory
from send_data import send_prices, send_production
from datetime import datetime, timedelta
import warnings
sys.path.append('O:/Codes')
from credentials import proxy_request, create_snowflake_connection, set_snowflake_context

warnings.filterwarnings("ignore")


ontem = datetime.now() - timedelta(days=1)
y_date = ontem.strftime("%d/%m/%Y")


BASE_URL = 'https://www.ec.europa.eu/agrifood'

def main(base_url: str, proxies: dict, date: str):
    send_prices(get_Livestock_Prices(base_url, proxies, date))
    send_production(get_Livestock_Production(base_url, proxies, date))
    send_inventory(get_Livestock_Inventory())

if __name__ == "__main__":
    try:
        proxies = proxy_request()
        main(BASE_URL, proxies, y_date)
    except Exception as e:
        print(f"[ERRO] Ocorreu um erro ao executar o main: {e}")
