import openpyxl


class Uruguay:
    @staticmethod
    def organize_Prices(path: str):
        wb_cattle_prices_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
        ws_cattle_prices_base = wb_cattle_prices_base['Base_Price_USD']

        wb_uruguay = openpyxl.load_workbook(f"{path}/Preços_Uruguai.xlsx")
        ws_uruguay = wb_uruguay.active
        data = ws_uruguay["A"]
        precos = ws_uruguay["B"]

        consolidated = {
            "Data": [],
            "Preços":[]
        }
        for a in data[1:]:
            consolidated["Data"].append(a.value)
        for c in precos[1:]:
            consolidated["Preços"].append(c.value)

        ws_cattle_prices_base['B1'] = 'USA - CWT/U$'
        ws_cattle_prices_base['C1'] = 'AUSTRALIA - AUD/KG'
        ws_cattle_prices_base['D1'] = 'BRAZIL - R$/@'
        ws_cattle_prices_base['E1'] = 'URUGUAY - U$/KG'
        ws_cattle_prices_base['F1'] = 'FRANCE - EUR/KG'
        ws_cattle_prices_base['G1'] = 'CANADA - CWT/CAD'
        ws_cattle_prices_base['H1'] = 'ARGENTINA - ARG/KG'
        ws_cattle_prices_base['I1'] = 'GREAT BRITAIN - P/KG'

        for Price,Line in zip(consolidated["Preços"][::], range(1, len(consolidated["Data"]))):
            try:
                ws_cattle_prices_base[f"E{Line+1}"] = (Price or 0)
            except:
                ws_cattle_prices_base[f"E{Line+1}"] = (Price or 0)
        
        wb_cattle_prices_base.save(f"{path}/Base.xlsx")
        print('Uruguay data organized')
    
    