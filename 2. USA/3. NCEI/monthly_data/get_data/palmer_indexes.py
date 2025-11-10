import pandas
import requests
import io

class Palmer_Indexes:
    @staticmethod
    def get_current_palmer(states: dict, palmer_indx: list, last_year: int, proxies: dict):
        try:
            lst_dfs = []

            for i in range(1,len(states)-1):
                for c in range(len(palmer_indx)):
                    url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/{palmer_indx[c]}/1/0/{last_year}-{last_year}/data.csv'
                    response = requests.get(url, proxies = proxies, verify = False)

                    df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                    df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                    df['State'] = states[i]
                    if palmer_indx[c] == 'pdsi':
                        df['Parameter'] = 'Palmer Drought Severity Index'
                    elif palmer_indx[c] == 'phdi':
                        df['Parameter'] = 'Palmer Hydrological Drought Index'
                    elif palmer_indx[c] == 'pmdi':
                        df['Parameter'] = 'Palmer Modified Drought Index'
                    else:
                        df['Parameter'] = 'Palmer Z-Index'

                    df['Unit'] = ''

                    df['Time_Scale'] = '1-Month'

                    df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                    lst_dfs.append(df)
                    c += 1
                i += 1

            df_final = pandas.concat(lst_dfs)
            print('Palmer indexes for the current year downloaded')
        except Exception as e:
            print(f"Error downloading palmer indexes: {e}")
        return df_final

    @staticmethod
    def get_historical_palmer(states: dict, palmer_indx: list, first_year: int, last_year: int, proxies: dict):
        lst_dfs = []

        for i in range(1,len(states)-1):
            for c in range(len(palmer_indx)):
                url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/statewide/time-series/{i}/{palmer_indx[c]}/1/0/{first_year}-{last_year}/data.csv'
                response = requests.get(url, proxies = proxies, verify = False)

                df = pandas.read_csv(io.StringIO(response.content.decode('utf-8')), comment='#')
                df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m')
                df['State'] = states[i]
                if palmer_indx[c] == 'pdsi':
                    df['Parameter'] = 'Palmer Drought Severity Index'
                elif palmer_indx[c] == 'phdi':
                    df['Parameter'] = 'Palmer Hydrological Drought Index'
                elif palmer_indx[c] == 'pmdi':
                    df['Parameter'] = 'Palmer Modified Drought Index'
                else:
                    df['Parameter'] = 'Palmer Z-Index'

                df['Unit'] = ''
                
                df['Time_Scale'] = '1-Month'

                df = df[['Date', 'State', 'Parameter', 'Unit', 'Time_Scale', 'Value']]

                lst_dfs.append(df)
                c += 1
            i += 1

        df_final = pandas.concat(lst_dfs)
        return df_final