from .collect_population import PlantPopCollector

def request_data(proxies: dict, year: int, api_key: str):
    corn_population = PlantPopCollector.collect_plant_population('corn', proxies, year, api_key)
    soybean_population = PlantPopCollector.collect_plant_population('soybean', proxies, year, api_key)

    return {
        'Corn_population'               :    corn_population,
        'Soybean_population'            :    soybean_population
    }