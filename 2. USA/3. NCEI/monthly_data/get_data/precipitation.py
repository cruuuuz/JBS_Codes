import pandas
import requests
import io

class Precipitation:
    @staticmethod
    def get_current_precipitation(states: dict, last_year: int, proxies: dict = None):
        try:
            lst_dfs = []

            for i in range(1,len(states)-1):
                url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/pcp/1/0/{last_year}-{last_year}/data.csv'
                response = requests.get(url, proxies = proxies, verify = False)

                df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                df['State'] = states[i]
                df['Parameter'] = 'Precipitation'
                df['Unit'] = 'Inches'

                df['Time_Scale'] = '1-Month'

                df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                lst_dfs.append(df)
                i += 1
            
            df_final = pandas.concat(lst_dfs)
            print('Precipitation data for the current year downloaded')
        except Exception as e:
            print(f'Error downloading precipitation data: {e}')
        return df_final

    @staticmethod
    def get_historical_precipitation(states: dict, first_year: int, last_year: int, proxies: dict = None):
        lst_dfs = []

        for i in range(1,len(states)-1):
            url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/pcp/1/0/{first_year}-{last_year}/data.csv'
            response = requests.get(url, proxies = proxies, verify = False)

            df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
            df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
            df['State'] = states[i]
            df['Parameter'] = 'Precipitation'
            df['Unit'] = 'Inches'

            df['Time_Scale'] = '1-Month'

            df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

            lst_dfs.append(df)
            i += 1
        
        df_final = pandas.concat(lst_dfs)
        return df_final