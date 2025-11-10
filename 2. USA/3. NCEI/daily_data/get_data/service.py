from .functions_weather_data import check_last_month, get_spot_weather_data

class Weather_Data:
    @staticmethod
    def get_weather_data(proxies: dict):
        check_last_month('prcp', proxies)
        check_last_month('tmin', proxies)
        check_last_month('tavg', proxies)
        check_last_month('tmax', proxies)
        return {
            'prcp' : get_spot_weather_data('prcp', proxies),
            'tmin' : get_spot_weather_data('tmin', proxies),
            'tmax' : get_spot_weather_data('tmax', proxies),
            'tavg' : get_spot_weather_data('tavg', proxies)
        }
