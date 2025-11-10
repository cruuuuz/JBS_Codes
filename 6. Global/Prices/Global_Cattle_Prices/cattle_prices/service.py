from .United_States import United_States 
from .Argentina import Argentina
from .Uruguay import Uruguay
from .Europe import CattlePricesEurope as Europe
from .Great_Britain import Great_Britain

def get_Global_Cattle_Prices(path: str, path_ams: str, proxies: dict, all_dates: dict):
    United_States.get_Prices(path, path_ams)
    Argentina.get_Prices(path, proxies)
    Uruguay.get_Prices(path, proxies)
    Great_Britain.get_Prices(path, proxies)