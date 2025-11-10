from .beef import Beef
from .pigmeat import Pigmeat
from .poultry import Poultry
from .eggs import Eggs

def get_all_production(base_url: str, proxies: dict, date: str):
    """
    Retorna todos os preços de proteína animal em uma lista de DataFrames.
    """
    return [
        Beef.get_Production(base_url, proxies, date),
        Pigmeat.get_Production(base_url, proxies, date),
        Poultry.get_Production(base_url, proxies, date)
    ]