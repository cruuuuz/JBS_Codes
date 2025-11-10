#CONVERTE PARA CWT/U$
import openpyxl
import pandas
import parse_Cattle_Data

cattle_prices_path = 'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Cattle_Prices_Teste'

wb_brazil = openpyxl.load_workbook(f'{cattle_prices_path}/Base_Cattle_Prices.xlsx')
ws_brazil = wb_brazil.active

wb_currency = openpyxl.load_workbook(f'{cattle_prices_path}/Base_Currency.xlsx')
ws_currency = wb_currency['Hist']

linha = 1
for i in range (1, 10000):
    try:
        ws_brazil[f"B{linha+1}"] = ws_brazil[f"B{linha+1}"].value #USA
        ws_brazil[f"C{linha+1}"] = ws_brazil[f"C{linha+1}"].value/0.6325*0.55 #Converte AUS
        ws_brazil[f"D{linha+1}"] = ws_brazil[f"D{linha+1}"].value #BRA
        ws_brazil[f"E{linha+1}"] = ws_brazil[f"E{linha+1}"].value/2.2046*100*0.535 #Converte Uruguay
        ws_brazil[f"F{linha+1}"] = ws_brazil[f"F{linha+1}"].value*ws_currency[f"D{linha+1}"].value/2.2046*0.58 #Converte France
        ws_brazil[f"G{linha+1}"] = ws_brazil[f"G{linha+1}"].value*ws_currency[f"I{linha+1}"].value*1.025 #Canada
        ws_brazil[f"H{linha+1}"] = ws_brazil[f"H{linha+1}"].value*ws_currency[f"E{linha+1}"].value/2.2046*100 #Converte Argentina

    except:
        ws_brazil[f"B{linha+1}"] = ws_brazil[f"B{linha+1}"].value
        ws_brazil[f"C{linha+1}"] = ws_brazil[f"C{linha+1}"].value
        ws_brazil[f"D{linha+1}"] = ws_brazil[f"D{linha+1}"].value
        ws_brazil[f"E{linha+1}"] = ws_brazil[f"E{linha+1}"].value
        ws_brazil[f"F{linha+1}"] = f"=F{linha+2}"
        ws_brazil[f"G{linha+1}"] = ws_brazil[f"G{linha+1}"].value
        ws_brazil[f"H{linha+1}"] = f"=H{linha+2}"

    linha += 1

df = pandas.DataFrame(ws_brazil.values)
df = df.drop(columns=[8])
df = df.drop(columns=[9])
df = df.drop(columns=[10])
df = df.drop(columns=[11])

df.columns = df.iloc[0,:]
df = df.drop(df.index[0])
df[f'{df.columns[1]}'] = pandas.to_numeric(df[f'{df.columns[1]}'], errors='coerce')
df[f'{df.columns[2]}'] = pandas.to_numeric(df[f'{df.columns[2]}'], errors='coerce')
df[f'{df.columns[3]}'] = pandas.to_numeric(df[f'{df.columns[3]}'], errors='coerce')
df[f'{df.columns[4]}'] = pandas.to_numeric(df[f'{df.columns[4]}'], errors='coerce')
df[f'{df.columns[5]}'] = pandas.to_numeric(df[f'{df.columns[5]}'], errors='coerce')
df[f'{df.columns[6]}'] = pandas.to_numeric(df[f'{df.columns[6]}'], errors='coerce')
df[f'{df.columns[7]}'] = pandas.to_numeric(df[f'{df.columns[7]}'], errors='coerce')
df[f'{df.columns[1]}'] = df[f'{df.columns[1]}'].replace(0,df.iloc[1,1])
df[f'{df.columns[2]}'] = df[f'{df.columns[2]}'].replace(0,df.iloc[1,2])
df[f'{df.columns[3]}'] = df[f'{df.columns[3]}'].replace(0,df.iloc[1,3])
df[f'{df.columns[4]}'] = df[f'{df.columns[4]}'].replace(0,df.iloc[1,4])
df[f'{df.columns[5]}'] = df[f'{df.columns[5]}'].replace(0,df.iloc[1,5])
df[f'{df.columns[6]}'] = df[f'{df.columns[6]}'].replace(0,df.iloc[1,6])
df[f'{df.columns[7]}'] = df[f'{df.columns[7]}'].replace(0,df.iloc[1,7])

df.loc[-1] = df.columns.tolist()
df.index = df.index + 1
df = df.sort_index()
df.columns = [0,1,2,3,4,5,6,7]
df = df.fillna(method='bfill')

df_cwt_friboi = df.head(500)
df_cwt = df.head(150)
df_cwt = df_cwt.drop(df.index[0])
df_cwt.columns = ["Dates", "United States", "Australia", "Brazil", "Uruguay", "France", "Canada", "Argentina"]
df.to_excel(f'{cattle_prices_path}/Global_Cattle_Prices.xlsx', header=False, index=False)