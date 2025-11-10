import openpyxl


class United_States:
    @staticmethod
    def organize_Prices(path: str):
        try:
            wb_cattle_prices_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
            ws_cattle_prices_base = wb_cattle_prices_base['Base_Price_USD']

            wb_usa = openpyxl.load_workbook(f'{path}/Preços_EUA.xlsx')
            ws_usa = wb_usa.active

            data = ws_usa["A"]
            precos = ws_usa["B"]

            consolidado_usa = {
                "Data": [],
                "Preços":[]
            }
            for a in data[1:]:
                consolidado_usa["Data"].append(a.value)
            for c in precos[1:]:
                consolidado_usa["Preços"].append(c.value)

            for Preco,Linha in zip(consolidado_usa["Preços"][::], range(1, len(consolidado_usa["Data"]))):
                try:
                    ws_cattle_prices_base[f"B{Linha+1}"] = (Preco or 0)
                except:
                    ws_cattle_prices_base[f"B{Linha+1}"] = (Preco or 0)
            
            wb_cattle_prices_base.save(f"{path}/Base.xlsx")
            print('USA data organized')
        except Exception as e:
            print('USA data do not organized')
    