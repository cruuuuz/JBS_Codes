import openpyxl
import pandas
import os


class Europe:
    @staticmethod
    def organize_Prices(path: str, all_dates: dict):
        wb_cattle_prices_base = openpyxl.load_workbook(f"{path}/Base.xlsx")
        ws_cattle_prices_base = wb_cattle_prices_base['Base_Price_USD']

        pasta_principal = f"{path}/Europa"

        valores_c26 = []

        # Percorre as pastas de ano
        for ano in os.listdir(pasta_principal):
            if os.path.isdir(os.path.join(pasta_principal, ano)):
                pasta_ano = os.path.join(pasta_principal, ano)

                # Percorre os arquivos .xlsx na pasta do ano
                for arquivo in os.listdir(pasta_ano):
                    if arquivo.endswith(".xlsx"):
                        caminho_arquivo = os.path.join(pasta_ano, arquivo)

                        try:
                            df = pandas.read_excel(caminho_arquivo, engine='openpyxl')
                            valor_c26 = df.iloc[25, 2] # A linha 26 corresponde ao índice 25, e a coluna C corresponde ao índice 2
                            valor_aa6 = df.iloc[4, 26]
                            valores_c26.append({valor_aa6 : valor_c26})
                            print(f'Adicionado {arquivo} de {ano}')
                        except Exception as e:
                            print(f"Erro ao ler o arquivo {arquivo} de {ano}: {e}. Preenchendo com 'None'.")
                            valor_c26 = None
                            valor_aa6 = None
                            valores_c26.append({valor_aa6 : valor_c26}) 

        dates = []
        prices = []

        for item in valores_c26:
            for date, price in item.items():
                dates.append(date)
                prices.append(price)

        df_final = pandas.DataFrame({'Date': dates, 'Price': prices})
        df_final['Price'] = df_final['Price'].fillna(method='ffill')
        df_final = df_final.sort_values(by='Date', ascending=False)

        for i in range(1,len(df_final)-2):
            try:
                ws_cattle_prices_base[f"F{i+3}"] = df_final.iloc[i,1]
                print('Europe parsed')
            except Exception as e:
                print(f'{e}. Linha: {i}')
            i += 1 

        wb_cattle_prices_base.save(f"{path}/Base.xlsx")

        print('Europe data organized')
        



# import pandas as pd
# import os

# # Pasta principal que contém as pastas dos anos
# pasta_principal = "O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Cattle_Prices_Teste/Europa" # Substitua pelo caminho da sua pasta

# # Lista para armazenar os valores da célula C26 de todos os arquivos
# valores_c26 = []

# # Percorre as pastas de ano
# for ano in os.listdir(pasta_principal):
#     if os.path.isdir(os.path.join(pasta_principal, ano)):
#         pasta_ano = os.path.join(pasta_principal, ano)

#         # Percorre os arquivos .xlsx na pasta do ano
#         for arquivo in os.listdir(pasta_ano):
#             if arquivo.endswith(".xlsx"):# and arquivo.startswith(ano): #garantir que só pega arquivos de ano certo
#                 caminho_arquivo = os.path.join(pasta_ano, arquivo)

#                 # Abre o arquivo e pega o valor da célula C26
#                 try:
#                     df = pandas.read_excel(caminho_arquivo, engine='openpyxl')
#                     valor_c26 = df.iloc[25, 2] # A linha 26 corresponde ao índice 25, e a coluna C corresponde ao índice 2
#                     valor_aa6 = df.iloc[4, 26]
#                     valores_c26.append({valor_aa6 : valor_c26})
#                     print(f'Adicionado {arquivo} de {ano}')
#                 except Exception as e:
#                     print(f"Erro ao ler o arquivo {arquivo} de {ano}: {e}")
#                     valor_c26 = None # A linha 26 corresponde ao índice 25, e a coluna C corresponde ao índice 2
#                     valor_aa6 = None
#                     valores_c26.append({valor_aa6 : valor_c26}) 

# dates = []
# prices = []

# for item in valores_c26:
#     for date, price in item.items():
#         dates.append(date)
#         prices.append(price)

# df_final = pandas.DataFrame({'Data': dates, 'Price': prices})
# df_final['Price'] = df_final['Price'].fillna(method='ffill')
# df_final = df_final[::-1]

# # Exibe o DataFrame
# print(df_final)

# # Se quiser salvar em um novo arquivo:
# # df_final.to_excel("valores_c26.xlsx", index=False)