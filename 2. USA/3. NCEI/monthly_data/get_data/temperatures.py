import pandas
import requests
import io

class Temperatures:
    @staticmethod
    def get_current_temperatures(states: dict, temp_parameters: list, last_year: int, proxies: dict = None):
        try:
            lst_dfs = []

            for i in range(1,len(states)-1):
                for c in range(len(temp_parameters)):
                    url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/{temp_parameters[c]}/1/0/{last_year}-{last_year}/data.csv'
                    response = requests.get(url, proxies = proxies, verify = False)

                    df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                    df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                    df['State'] = states[i]
                    if temp_parameters[c] == 'tavg':
                        df['Parameter'] = 'Average Temperature'
                    elif temp_parameters[c] == 'tmax':
                        df['Parameter'] = 'Maximum Temperature'
                    else:
                        df['Parameter'] = 'Minimum Temperature'

                    df['Unit'] = 'Degrees Fahrenheit'

                    df['Time_Scale'] = '1-Month'

                    df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                    lst_dfs.append(df)
                    c += 1
                i += 1

            df_final = pandas.concat(lst_dfs)
            print('Temperature data for the current year downloaded')
        except Exception as e:
            print(f'Error downloading temperature data: {e}')
        return df_final

    @staticmethod
    def get_historical_temperatures(states: dict, temp_parameters: list, first_year: int, last_year: int, proxies: dict = None):
        lst_dfs = []

        for i in range(1,len(states)-1):
            for c in range(len(temp_parameters)):
                url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/{temp_parameters[c]}/1/0/{first_year}-{last_year}/data.csv'
                response = requests.get(url, proxies = proxies, verify = False)

                df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                df['State'] = states[i]
                if temp_parameters[c] == 'tavg':
                    df['Parameter'] = 'Average Temperature'
                elif temp_parameters[c] == 'tmax':
                    df['Parameter'] = 'Maximum Temperature'
                else:
                    df['Parameter'] = 'Minimum Temperature'

                df['Unit'] = 'Degrees Fahrenheit'

                df['Time_Scale'] = '1-Month'

                df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                lst_dfs.append(df)
                c += 1
            i += 1

        df_final = pandas.concat(lst_dfs)
        return df_final

