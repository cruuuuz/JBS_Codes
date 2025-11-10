import pandas
import requests
import io

class Degree_Days:
    @staticmethod
    def get_current_degree(states: dict, degree_measures: list, last_year: int, proxies: dict = None):
        try:
            lst_dfs = []

            for i in range(1,len(states)-1):
                for c in range(len(degree_measures)):
                    url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/{degree_measures[c]}/1/0/{last_year}-{last_year}/data.csv'
                    response = requests.get(url, proxies = proxies, verify = False)

                    df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                    df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                    df['State'] = states[i]
                    if degree_measures[c] == 'cdd':
                        df['Parameter'] = 'Cooling Degree Days'
                        df['Unit'] = 'CDD'
                    else:
                        df['Parameter'] = 'Heating Degree Days'
                        df['Unit'] = 'HDD'

                    df['Time_Scale'] = '1-Month'

                    df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                    lst_dfs.append(df)
                    c += 1
                i += 1

            df_final = pandas.concat(lst_dfs)
            print('Degree days data for the current year downloaded')
        except Exception as e:
            print(f'Degree days for current year error. Error: {e}')
        return df_final

    @staticmethod
    def get_historical_degree(states: dict, degree_measures: list, first_year: int, last_year: int, proxies: dict = None):
        lst_dfs = []

        for i in range(1,len(states)-1):
            for c in range(len(degree_measures)):
                url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/{degree_measures[c]}/1/0/{first_year}-{last_year}/data.csv'
                response = requests.get(url, proxies = proxies, verify = False)

                df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                df['State'] = states[i]
                if degree_measures[c] == 'cdd':
                    df['Parameter'] = 'Cooling Degree Days'
                    df['Unit'] = 'CDD'
                else:
                    df['Parameter'] = 'Heating Degree Days'
                    df['Unit'] = 'HDD'

                df['Time_Scale'] = '1-Month'

                df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                lst_dfs.append(df)
                c += 1
            i += 1

        df_final = pandas.concat(lst_dfs)
        return df_final