import openpyxl
import pandas


class Great_Britain:
    @staticmethod
    def organize_Prices(path: str):
        wb_cattle_prices_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
        ws_cattle_prices_base = wb_cattle_prices_base['Base_Price_USD']

        wb_great_britain = openpyxl.load_workbook(f'{path}/Preços_Great_Britain.xlsx')
        ws_great_britain = wb_great_britain.active

        data = ws_great_britain["A"]
        precos = ws_great_britain["D"]
        
        consolidado_great_britain = {
            "Data": [],
            "Preços":[]
        }
        for a in data[1:]:
            consolidado_great_britain["Data"].append(a.value)
        for c in precos[1:]:
            consolidado_great_britain["Preços"].append(c.value)
            
        for Preco,Linha in zip(consolidado_great_britain["Preços"][::], range(1, len(consolidado_great_britain["Data"]))):
            try:
                ws_cattle_prices_base[f"I{Linha+1}"] = (Preco or 0)
            except:
                ws_cattle_prices_base[f"I{Linha+1}"] = (Preco or 0)
        
        wb_cattle_prices_base.save(f"{path}/Base.xlsx")
        print('Great Britain data organized')