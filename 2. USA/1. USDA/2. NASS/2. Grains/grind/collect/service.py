from .collect_grinds import GrindCollector

def request_data(proxies: dict, year: int, api_key: str):
    corn_usage = GrindCollector.collect_grind('corn_usage', proxies, year, api_key)
    alcohol_coproducts = GrindCollector.collect_grind('alcohol_coproducts', proxies, year, api_key)

    return {
        'Corn_usage'               :    corn_usage,
        'Alcohol'                  :    alcohol_coproducts,
    }