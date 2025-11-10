from .collect_data import ABSCollector 

def request_data(proxies: dict, year: int):
    data = ABSCollector.collect_slaughter_production(proxies, year)
    slaughter = data[0]
    production = data[1]

    return {
        'Slaughter'               :    slaughter,
        'Production'              :    production
    }