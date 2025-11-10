import pandas
import requests
import datetime
import openpyxl


class Argentina:
    @staticmethod
    def get_Prices(path: str, proxies: dict):
        try:
            wb_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
            ws_base = wb_base['Base_Price_USD']
            
            inicio, fim = "01/01/2011", datetime.datetime.now().strftime("%d/%m/%Y")
            url = f"http://www.mercadoagroganadero.com.ar/php/hacigraf000110.chartjs.php?txtFECHAINI={inicio}&txtFECHAFIN={fim}&txtCLASE=1"
            response = requests.get(url, proxies=proxies)
            dados = response.json()
            dates = [string.split()[1] for string in dados['labels']]
            dates = dates[::-1]
            precos = dados['data'][0]['vals'][::-1]

            preco_argetina = pandas.DataFrame({'Date': dates, 'Price': precos})
            preco_argetina['Date'] = pandas.to_datetime(preco_argetina['Date'], dayfirst=True)
            preco_argetina = preco_argetina.loc[preco_argetina['Date'].dt.weekday == 4]
            preco_argetina = preco_argetina.reset_index()
            preco_argetina = preco_argetina[['Date','Price']]

            dates_excel = []
            for i in range(1, len(preco_argetina)):
                dates_excel.append(ws_base[f"A{i+1}"].value)
            df_excel = pandas.DataFrame(dates_excel)
            df_excel.columns = ['Date']
            result_argentina = pandas.merge(df_excel, preco_argetina, on='Date', how='left')

            for i in range(1, len(preco_argetina)):
                try:
                    ws_base[f"H{i+1}"] = (result_argentina['Price'][i-1] or 0)
                except:
                    ws_base[f"H{i+1}"] = (result_argentina['Price'][i-1] or 0)
                i += 1
            preco_argetina.to_excel(f'{path}/Precos_Argentina.xlsx', index=False)
            print('Argentina data downloaded')
        except Exception as e:
            print(f'Error downloading Argentina prices: {e}')