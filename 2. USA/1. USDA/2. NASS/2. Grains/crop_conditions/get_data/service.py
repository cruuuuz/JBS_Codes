from .get_Conditions import Conditions

def request_data(proxies: dict, year: int, api_key: str):
    corn_conditions = Conditions.Corn_Condition_Data(proxies, year, api_key)
    pasture_conditions = Conditions.Pasture_Condition_Data(proxies, year, api_key)
    soybean_conditions = Conditions.Soybean_Condition_Data(proxies, year, api_key)
    wheat_conditions = Conditions.Wheat_Condition_Data(proxies, year, api_key)

    return {
        'Pasture_Conditions'    :    pasture_conditions,
        'Corn_Conditions'       :    corn_conditions,
        'Soybean_Conditions'    :    soybean_conditions,
        'Wheat_Conditions'      :    wheat_conditions,
    }