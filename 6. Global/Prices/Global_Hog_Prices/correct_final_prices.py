import pandas
import openpyxl



def adjust_price(df):
    for col in df.columns:
        if pandas.api.types.is_numeric_dtype(df[col]):
            for i in range(len(df)-1):
                if df.at[i,col] * 0.96 > df.at[i+1,col]:
                    df.at[i, col] = df.at[i + 2, col]
    return df

df_europe_prices = pandas.read_excel(f'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices/Global_Swine_Prices_Weekly.xlsx', sheet_name='Europe_Swine_Prices')
df_europe_prices = adjust_price(df_europe_prices)
df_europe_prices = df_europe_prices.to_excel(f'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices/Europe_Swine_Prices_Weekly.xlsx', index=False)
print('Europe prices adjusts done')

df_asian_prices = pandas.read_excel(f'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices/Asian_Hogs_Prices.xlsx')
df_asian_prices = adjust_price(df_asian_prices)
df_asian_prices = df_asian_prices.to_excel(f'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices/Asian_Hogs_Prices.xlsx', index=False)
print('Asian prices adjusts done')

df_americas_prices = pandas.read_excel(f'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices/Americas_Hogs_Prices.xlsx')
df_americas_prices = adjust_price(df_americas_prices)
df_americas_prices = df_americas_prices.to_excel(f'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices/Americas_Hogs_Prices.xlsx', index=False)
print('American prices adjusts done')



