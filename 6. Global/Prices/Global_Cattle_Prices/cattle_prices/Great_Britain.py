import requests
import pandas


class Great_Britain:
    @staticmethod
    def get_Prices(path: str, proxies: dict):
        try:
            url_great_britain = 'https://projectblue.blob.core.windows.net/media/Default/Market%20Intelligence/beef/D&A/Prices/Deadweight/Weekly%20deadweight%20cattle%20prices.xlsx'
            response_xls = requests.get(url_great_britain, proxies=proxies, verify=False)

            with open(f'{path}/Preços_Great_Britain.xlsx','wb') as f:
                f.write(response_xls.content)

            df_great_britain = pandas.read_excel(f"{path}/Preços_Great_Britain.xlsx", sheet_name='Young bulls')
            df_great_britain = pandas.DataFrame(df_great_britain)
            for i in range(5):
                df_great_britain = df_great_britain.drop(i)
            df_great_britain.columns = df_great_britain.iloc[0]
            df_great_britain = df_great_britain.drop(5)
            df_great_britain = df_great_britain[['Week ending', 'Category', 'Region', 'Overall Price']]

            df_great_britain.columns = ['Dates', 'Category', 'Region', 'P/Kg']

            df_great_britain = df_great_britain[df_great_britain['Region'] == 'Great Britain']
            df_great_britain = df_great_britain.sort_values(by='Dates', ascending=False)

            df_great_britain.to_excel(f'{path}/Preços_Great_Britain.xlsx', header=True, index=False)
            print("Great Britain prices downloaded")
        except Exception as e:
            print(f"Error downloading Great Britain prices: {e}")