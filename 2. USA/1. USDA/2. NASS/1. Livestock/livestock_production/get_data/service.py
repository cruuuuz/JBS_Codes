from .get_Slaughter import Slaughter
from .get_Production import Production
from .get_Weights import Weights

def request_data(proxies: dict, year: int, api_key: str):
    cattle_slaughter = Slaughter.Cattle_Slaughter_Data(proxies, year, api_key)
    hog_slaughter = Slaughter.Hog_Slaughter_Data(proxies, year, api_key)
    beef_production = Production.Beef_Production_Data(proxies, year, api_key)
    pork_production = Production.Pork_Production_Data(proxies, year, api_key)
    cattle_weights = Weights.Cattle_Weight_Data(proxies, year, api_key)
    hog_weights = Weights.Hog_Weight_Data(proxies, year, api_key)

    return {
        'Cattle_Slaughter'   :    cattle_slaughter,
        'Hog_Slaughter'      :    hog_slaughter,
        'Beef_Production'    :    beef_production,
        'Pork_Production'    :    pork_production,
        'Cattle_Weights'     :    cattle_weights,
        'Hog_Weights'        :    hog_weights
    }