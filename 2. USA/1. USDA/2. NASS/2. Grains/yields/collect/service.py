from .collect_yields import YieldCollector

def request_data(proxies: dict, year: int, api_key: str):
    corn_yield = YieldCollector.collect_yield('corn', proxies, year, api_key)
    spring_wheat_yield = YieldCollector.collect_yield('spring_wheat_excl_durum', proxies, year, api_key)
    spring_wheat_durum_yield = YieldCollector.collect_yield('spring_wheat_durum', proxies, year, api_key)
    winter_wheat_yield = YieldCollector.collect_yield('winter_wheat', proxies, year, api_key)
    soybeans_yield = YieldCollector.collect_yield('soybeans', proxies, year, api_key)

    return {
        'Corn_yield'               :    corn_yield,
        'Soybean'                  :    soybeans_yield,
        'Spring_Wheat_ex_durum'    :    spring_wheat_yield,
        'Spring_Wheat_Durum'       :    spring_wheat_durum_yield,
        'Winter_Wheat'             :    winter_wheat_yield
    }