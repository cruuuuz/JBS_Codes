import openpyxl
import pandas


class Canada:
    @staticmethod
    def organize_Prices(path: str):
        wb_cattle_prices_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
        ws_cattle_prices_base = wb_cattle_prices_base['Base_Price_USD']

        wb_canada = openpyxl.load_workbook(f"{path}/Preços_Canada.xlsx")
        ws_canada = wb_canada.active
        data = ws_canada["A"]
        precos = ws_canada["B"]

        consolidado_canada = {
            "Data": [],
            "Preços":[]
        }
        for a in data[1:]:
            consolidado_canada["Data"].append(a.value)
        for c in precos[1:]:
            consolidado_canada["Preços"].append(c.value)

        for Preco,Linha in zip(consolidado_canada["Preços"][::], range(1, len(consolidado_canada["Data"]))):
            try:
                ws_cattle_prices_base[f"G{Linha+1}"] = (Preco or 0)
            except:
                ws_cattle_prices_base[f"G{Linha+1}"] = (Preco or 0)
                

        df = pandas.DataFrame(ws_cattle_prices_base.values)
        df.to_excel(f'{path}/Base_Cattle_Prices.xlsx', header=False, index=False)
        print('Canada data organized')