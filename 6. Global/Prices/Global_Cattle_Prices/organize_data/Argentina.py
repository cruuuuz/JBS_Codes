import openpyxl

class Argentina:
    @staticmethod
    def organize_Prices(path: str):
        wb_cattle_prices_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
        ws_cattle_prices_base = wb_cattle_prices_base['Base_Price_USD']

        wb_arg = openpyxl.load_workbook(f'{path}/Precos_Argentina.xlsx')
        ws_arg = wb_arg.active

        data = ws_arg["A"]
        precos = ws_arg["B"]
        
        consolidado_arg = {
            "Data": [],
            "Preços":[]
        }
        for a in data[1:]:
            consolidado_arg["Data"].append(a.value)
        for c in precos[1:]:
            consolidado_arg["Preços"].append(c.value)
            
        for Preco,Linha in zip(consolidado_arg["Preços"][::], range(1, len(consolidado_arg["Data"]))):
            try:
                ws_cattle_prices_base[f"H{Linha+1}"] = (Preco or 0)
            except:
                ws_cattle_prices_base[f"H{Linha+1}"] = (Preco or 0)
        
        wb_cattle_prices_base.save(f"{path}/Base.xlsx")
        print('Argentina data organized')
        