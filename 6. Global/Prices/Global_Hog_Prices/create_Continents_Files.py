import pandas

path = 'O:/Cruz/Inteligencia/Acompanhamento Global de Preços/Hog_Prices'

#Americas
df_cop = pandas.read_excel(f'{path}/SwinePrices_86.xlsx')
df_col = pandas.read_excel(f'{path}/SwinePrices_148.xlsx')
df_arg = pandas.read_excel(f'{path}/SwinePrices_149.xlsx')

df_americas = pandas.concat([df_cop, df_col['Prices']], axis=1)
df_americas = pandas.concat([df_americas, df_arg['Prices']], axis=1)
df_americas = df_americas.dropna()
df_americas.columns = ['Date', 'Chile', 'Colombia', 'Argentina']
df_americas['Chile'] = df_americas['Chile'].str.replace(',','.')
df_americas['Chile'] = pandas.to_numeric(df_americas['Chile'])/0.75
df_americas['Colombia'] = df_americas['Colombia'].str.replace(',','.')
df_americas['Colombia'] = pandas.to_numeric(df_americas['Colombia'])/0.75
df_americas['Argentina'] = df_americas['Argentina'].str.replace(',','.')
df_americas['Argentina'] = pandas.to_numeric(df_americas['Argentina'])/0.75
df_americas.to_excel(f'{path}/Americas_Hogs_Prices.xlsx', index=False, header=True)


#Asia
df_jpy = pandas.read_excel(f'{path}/SwinePrices_173.xlsx')
df_jpy = df_jpy.dropna()

df_vie = pandas.read_excel(f'{path}/SwinePrices_166.xlsx')
df_vie['Year'] = df_vie['Date'].str.split('/').str[0]
df_2024 = df_vie[df_vie['Year'] == '2024']
df_2024 = df_2024.iloc[::7]
df_vie = df_vie[df_vie['Year'] != '2024'].dropna()
df_vie = pandas.concat([df_2024, df_vie], axis=0)
df_vie = df_vie.drop(columns='Year').reset_index(drop=True)


df_skr = pandas.read_excel(f'{path}/SwinePrices_90.xlsx')
df_skr = df_skr.iloc[::7].reset_index()
df_twn = pandas.read_excel(f'{path}/SwinePrices_174.xlsx')
df_twn = df_twn.iloc[::7].reset_index()
df_asia = pandas.concat([df_jpy, df_vie['Prices']], axis=1)
df_asia = pandas.concat([df_asia, df_skr['Prices']], axis=1)
df_asia = pandas.concat([df_asia, df_twn['Prices']], axis=1)

df_asia.columns = ['Date', 'Japan', 'Vietnan', 'South Korea', 'Taiwan']
df_asia['Vietnan'] = df_asia['Vietnan'].str.replace(',','.')
df_asia['Vietnan'] = pandas.to_numeric(df_asia['Vietnan'])/0.75
df_asia['Taiwan'] = df_asia['Taiwan'].str.replace(',','.')
df_asia['Taiwan'] = pandas.to_numeric(df_asia['Taiwan'])/0.75
df_asia.to_excel(f'{path}/Asian_Hogs_Prices.xlsx', index=False, header=True)







