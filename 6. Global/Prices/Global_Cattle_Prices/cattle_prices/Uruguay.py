import requests
import pandas


class Uruguay:
    @staticmethod
    def get_Prices(path: str, proxies: dict):
        try:
            url_uruguay = 'https://www.inac.uy/innovaportal/file/10952/1/webinac---serie-semanal-precios-de-hacienda.xlsx'
            response_xls = requests.get(url_uruguay, proxies=proxies)

            with open(f'{path}/Preços_Uruguai.xlsx','wb') as f:
                f.write(response_xls.content)

            df_uruguay = pandas.read_excel(f"{path}/Preços_Uruguai.xlsx")
            df_uruguay = pandas.DataFrame(df_uruguay)
            df_uruguay = df_uruguay[['SERIE', 'Unnamed: 9', 'Unnamed: 11']]

            i = 1
            for i in range(0,113): #line 113 is the first date with recorded yields
                df_uruguay = df_uruguay.drop(i)
                i +=1


            lst = df_uruguay.values.tolist()

            df_uruguay = pandas.DataFrame(reversed(lst))
            df_uruguay.columns = ['Dates', 'UD$/Kg', 'Rendimento']

            df_uruguay.to_excel(f'{path}/Preços_Uruguai.xlsx', header=True, index=False)
            print("Uruguay prices downloaded")
        except Exception as e:
            print(f"Error downloading Uruguay prices: {e}")