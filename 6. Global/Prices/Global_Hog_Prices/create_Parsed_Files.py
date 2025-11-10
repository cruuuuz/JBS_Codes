'''
# Objective: Collect and analyze historical hog prices globally.

# Techniques used:
# - Web Scraping: Extract data from websites like Pig333, European Union, and AHDB.
# - Data manipulation: Clean, process, and organize data with Pandas and Openpyxl.
# - Data analysis: Calculate averages and convert units of measure.
# - Data visualization: Create charts with Matplotlib and Mpld3.
# - Automation: Send emails with Outlook and update Snowflake database.


# Phase 3: Data analysis and visualization
# - Calculate weighted average prices for the European Union.
# - Create charts to visualize global price trends.
# - Generate separate charts for hog prices in the Americas and Asia.

# Phase 4: Automation
# - Send an email with a chart of global hog prices.
# - Update a Snowflake database with the collected and processed data.
'''

import pandas, openpyxl, sys
sys.path.append('O:/Cruz/Codes/Inteligência')
from password import password

path = 'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices'
path_cattle = 'O:/Cruz'

df_main_global_prices = pandas.read_excel(f"{path}/Base_Global_Swine_Prices.xlsx")
df_main_global_prices = df_main_global_prices.head(1200)
df_main_global_prices = df_main_global_prices.dropna()
df_main_global_prices = df_main_global_prices[['Date', 'USA', 'BRA', 'CAN', 'SPAIN', 'MEX', 'CHINA']]
df_main_global_prices.to_excel(f"{path}/Global_Swine_Prices_Weekly.xlsx", index=False)

wb_europa_final = openpyxl.load_workbook(f"{path}/Global_Swine_Prices_Weekly.xlsx")
ws_europa_final = wb_europa_final.create_sheet(title="Europe_Swine_Prices")
ws_europa_final = wb_europa_final["Europe_Swine_Prices"]

wb_eur = openpyxl.load_workbook(f"{path}/EU_Hog_Prices_Weekly.xlsx")
ws_eur = wb_eur.active

wb_temp = openpyxl.load_workbook(f"{path}/Base_Global_Swine_Prices.xlsx")
ws_temp = wb_temp.active

linha = 0
for i in range(0, 1000):
    try:
        ws_europa_final[f"A{linha+1}"] = ws_eur[f"A{linha+1}"].value
        ws_europa_final[f"B1"] = ws_eur[f"B1"].value
        ws_europa_final[f"C1"] = ws_eur[f"C1"].value
        ws_europa_final[f"D1"] = ws_eur[f"D1"].value
        ws_europa_final[f"E1"] = ws_eur[f"E1"].value
        ws_europa_final[f"F1"] = ws_eur[f"F1"].value
        ws_europa_final[f"G1"] = ws_eur[f"G1"].value
        ws_europa_final[f"H1"] = ws_eur[f"H1"].value
        ws_europa_final[f"I1"] = ws_eur[f"I1"].value
        ws_europa_final[f"J1"] = ws_temp[f"O1"].value
        ws_europa_final[f"K1"] = ws_temp[f"P1"].value
        ws_europa_final[f"B{linha+1}"] = ws_temp[f"D{linha+1}"].value
        ws_europa_final[f"C{linha+1}"] = ws_temp[f"E{linha+1}"].value
        ws_europa_final[f"D{linha+1}"] = ws_temp[f"F{linha+1}"].value
        ws_europa_final[f"E{linha+1}"] = ws_temp[f"G{linha+1}"].value
        ws_europa_final[f"F{linha+1}"] = ws_temp[f"H{linha+1}"].value
        ws_europa_final[f"G{linha+1}"] = ws_temp[f"I{linha+1}"].value
        ws_europa_final[f"H{linha+1}"] = ws_temp[f"J{linha+1}"].value
        ws_europa_final[f"I{linha+1}"] = ws_temp[f"N{linha+1}"].value
        ws_europa_final[f"J{linha+1}"] = ws_temp[f"O{linha+1}"].value
        ws_europa_final[f"K{linha+1}"] = ws_temp[f"P{linha+1}"].value
    except Exception as e:
        print(f'error while data was being processed: {e}')
    linha += 1

wb_europa_final.save(f"{path}/Global_Swine_Prices_Weekly.xlsx")