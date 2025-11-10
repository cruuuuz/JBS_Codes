from .collect_harvest import HarvestCollector
from .collect_planted import PlantedCollector

def request_data(proxies: dict, year: int, api_key: str):
    products = {
        'Corn'                    : 'corn',
        'Soybean'                 : 'soybeans',
        'Spring_Wheat_ex_durum'   : 'spring_wheat_excl_durum',
        'Spring_Wheat_Durum'      : 'spring_wheat_durum',
        'Winter_Wheat'            : 'winter_wheat',
        'Barley'                  : 'barley',
        'Oats'                    : 'oats',
        'Sorghum'                 : 'sorghum',
        'Rice'                    : 'rice',
        'Sunflower'               : 'sunflower',
        'Canola'                  : 'canola',
        'Cotton'                  : 'cotton',
        'Peanut'                  : 'peanut',
        'Sugarbeet'               : 'sugarbeets'
    }

    harvested_data = {}
    planted_data = {}

    for name, code in products.items():
        harvested_data[name] = HarvestCollector.collect_harvest(code, proxies, year, api_key)
        planted_data[name] = PlantedCollector.collect_planted(code, proxies, year, api_key)

    return {
        'harvested': harvested_data,
        'planted': planted_data
    }