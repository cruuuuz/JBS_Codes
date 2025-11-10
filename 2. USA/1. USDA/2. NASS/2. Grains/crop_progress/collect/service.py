from .collect_progress import ProgressCollector

def request_data(proxies: dict, year: int, api_key: str):
    corn_progress = ProgressCollector.collect_progress('corn', proxies, year, api_key)
    spring_wheat_progress = ProgressCollector.collect_progress('spring_wheat_excl_durum', proxies, year, api_key)
    spring_wheat_durum_progress = ProgressCollector.collect_progress('spring_wheat_durum', proxies, year, api_key)
    winter_wheat_progress = ProgressCollector.collect_progress('winter_wheat', proxies, year, api_key)
    soybeans_progress = ProgressCollector.collect_progress('soybeans', proxies, year, api_key)

    return {
        'Corn_Progress'            :    corn_progress,
        'Soybean'                  :    soybeans_progress,
        'Spring_Wheat_ex_durum'    :    spring_wheat_progress,
        'Spring_Wheat_Durum'       :    spring_wheat_durum_progress,
        'Winter_Wheat'             :    winter_wheat_progress
    }