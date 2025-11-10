from .collect_ears_pods import EarPodCollector

def request_data(proxies: dict, year: int, api_key: str):
    corn_ear_count = EarPodCollector.collect_ear_pod('corn_ear_count', proxies, year, api_key)
    soybean_pod_count = EarPodCollector.collect_ear_pod('soybeans_pod_count', proxies, year, api_key)

    return {
        'Corn'               :    corn_ear_count,
        'Soybean'            :    soybean_pod_count,
    }