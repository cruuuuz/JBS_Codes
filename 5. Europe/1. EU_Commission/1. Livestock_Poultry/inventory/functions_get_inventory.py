import requests 
import sys
import os
import gzip
import shutil
import csv
import pandas
import win32com.client as win32
import time
import snowflake.connector
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Cruz/Codes/Inteligência')
from password import password
from snowflake_connector import snowflake_connector


proxies = {
        'http' : f'http://{password()}@MTZSVMFCPPRD02:8080',
        'https' : f'http://{password()}@MTZSVMFCPPRD02:8080'
    }

# snowflake_connector = snowflake_connector()
# cnn = snowflake.connector.connect(
# user = snowflake_connector['user'],
# password = snowflake_connector['password'],
# account = snowflake_connector['account'],
# proxy_host = snowflake_connector['proxy_host'],
# proxy_port = snowflake_connector['proxy_port']
# )

destination_path = 'O:/Cruz/Inteligencia/Karl/5. Europa/1. Supply'


############################################################################################################################################################################################
#Link de sumários
# 'https://db.nomics.world/Eurostat/apro_mt_pwgtm?dimensions=%7B%22geo%22%3A%5B%22BE%22%5D%7D&tab=table'
#Link da base de dados
# 'https://ec.europa.eu/eurostat/web/main/data/database'
#THS_HD = thousand heads
#THS_T = thousand Tons
#SL = Slaughterings
############################################################################################################################################################################################



meat_products = {
    'B1000'      : 'Bovine_Meat',
    'B1100'      : 'Calf_and_Young_Cattley',
    'B1110'      : 'Calf',
    'B1120'      : 'Young_Cattle',
    'B1200'      : 'Adult_Cattle',
    'B1210'      : 'Bullock',
    'B1210_1220' : 'Bullock_and_Bull',
    'B1220'      : 'Bull',
    'B1230'      : 'Cow',
    'B1240'      : 'Heifer',
    'B3100'      : 'Pigmeat',
    'B4000'      : 'Meat_Sheep_and_Goats',
    'B4100'      : 'Sheepmeat',
    'B4110'      : 'Lamb',
    'B4120'      : 'Mutton',
    'B4200'      : 'Goat_Meat',
    'B5000'      : 'Horse_Meat',
    'B7000'      : 'Poultry_Meat',
    'B7100'      : 'Chicken',
    'B7200'      : 'Duck',
    'B7300'      : 'Turkey',
    'B7490'      : 'Other_Poultry',
}
pig_codes = {
    'A3100'      : 'Live swine, domestic species',
    'A3110'      : 'Piglets, live weight of under 20 kg',
    'A3120_3133' : 'Breeding pigs',
    'A3120'      : 'Breeding sows, live weight 50 kg or over',
    'A3120K'     : 'Covered sows',
    'A3120KA'    : 'Sows covered for the first time',
    'A3120L'     : 'Sows, not covered',
    'A3120LA'    : 'Gilts not yet covered',
    'A3131'      : 'Pigs, from 20 kg to less than 50 kg',
    'A3132'      : 'Fattening pigs, live weight 50 kg or over',
    'A3132X'     : 'Fattening pigs, from 50 kg to less than 80 kg',
    'A3132Y'     : 'Fattening pigs, from 80 kg to less than 110 kg',
    'A3132Z'     : 'Fattening pigs, live weight 110 kg or over',
    'A3133'      : 'Breeding boars'
}
countries_codes = {
    'EU27_2020'    :   'European Union - 27 countries (from 2020)',
    'EU28'         :   'European Union - 28 countries (2013-2020)',
    'EU27_2007'    :   'European Union - 27 countries (2007-2013)',
    'EU25'         :   'European Union - 25 countries (2004-2006)',
    'EU15'         :   'European Union - 15 countries (1995-2004)',
    'BE'           :   'Belgium',
    'BG'           :   'Bulgaria',
    'CZ'           :   'Czechia',
    'DK'           :   'Denmark',
    'DE'           :   'Germany',
    'EE'           :   'Estonia',
    'IE'           :   'Ireland',
    'EL'           :   'Greece',
    'ES'           :   'Spain',
    'FR'           :   'France',
    'HR'           :   'Croatia',
    'IT'           :   'Italy',
    'CY'           :   'Cyprus',
    'LV'           :   'Latvia',
    'LT'           :   'Lithuania',
    'LU'           :   'Luxembourg',
    'HU'           :   'Hungary',
    'MT'           :   'Malta',
    'NL'           :   'Netherlands',
    'AT'           :   'Austria',
    'PL'           :   'Poland',
    'PT'           :   'Portugal',
    'RO'           :   'Romania',
    'SI'           :   'Slovenia',
    'SK'           :   'Slovakia',
    'FI'           :   'Finland',
    'SE'           :   'Sweden',
    'IS'           :   'Iceland',
    'CH'           :   'Switzerland',
    'UK'           :   'United Kingdom',
    'BA'           :   'Bosnia and Herzegovina',
    'ME'           :   'Montenegro',
    'MK'           :   'North Macedonia',
    'AL'           :   'Albania',
    'RS'           :   'Serbia',
    'TR'           :   'Türkiye',
    'XK'           :   'Kosovo'
}
bovine_codes = {
    'A2000'           :   'Live bovine animals',
    'A2010'           :   'Bovine animals, less than 1 year old',
    'A2010B'          :   'Bovine animals, less than 1 year old, for slaughter',
    'A2010C'          :   'Bovine animals, less than 1 year old, not for slaughter',
    'A2020'           :   'Bovine animals, 1 to less than 2 years old',
    'A2030'           :   'Bovine animals, 2 years old or over',
    'A2110C'          :   'Male calves, less than 1 year old, not for slaughter',
    'A2120'           :   'Male bovine animals, 1 to less than 2 years old',
    'A2130'           :   'Male bovine animals, 2 years old or over',
    'A2230_2330'      :   'Female bovine animals, 2 years old or over',
    'A2210C'          :   'Female calves, less than 1 year old, not for slaughter',
    'A2220'           :   'Heifers, 1 to less than 2 years old',
    'A2220B'          :   'Heifers, 1 year old, for slaughter',
    'A2220C'          :   'Heifers, 1 year old, not for slaughter',
    'A2230'           :   'Heifers, 2 years old or over',
    'A2230B'          :   'Heifers, 2 years old or over, for slaughter',
    'A2230C'          :   'Heifers, 2 years old or over, not for slaughter',
    'A2300'           :   'Cows',
    'A2300F'          :   'Dairy cows',
    'A2300G'          :   'Non dairy cows',
    'A2400'           :   'Buffaloes',
    'A2410'           :   'Breeding female buffaloes',
    'A2420'           :   'Other buffaloes'
}
poultry_hatch_item_codes = {
    'CH'       :  'Chicks hatched',
    'CP'       :  'Chicks placed',
    'EPI'      :  'Eggs placed in incubation',
    'EXP'      :  'Exports',
    'EXP_IEU'  :  'Exports intra-EU',
    'EXP_XEU'  :  'Exports extra-EU',
    'ET'       :  'Exports - Total',
    'IMP'      :  'Imports',
    'IMP_IEU'  :  'Imports intra-EU',
    'IMP_XEU'  :  'Imports extra-EU',
    'IT'       :  'Imports - Total',
    'CU'       :  'Chicks for use'
}
poultry_codes = {
    'A5130O'   : 'Chicks of laying hen breeds (laying)',
    'A5130ON'  : 'Chicks of laying hen breeds (selection)',
    'A5130P'   : 'Chicks of meat broiler breeds (fattening)',
    'A5130PN'  : 'Chicks of meat broiler breeds (selection)',
    'A5130M'   : 'Chicks of mixed meat-laying breeds',
    'A5131P'   : 'Cockerels from sexing',
    'A5211P'   : 'Chicks of duck (fattening)',
    'A5221P'   : 'Chicks of goose (fattening)',
    'A5231P'   : 'Chicks of turkey (fattening)',
    'A5241P'   : 'Chicks of Guinea fowls (fattening)'
}
milk_codes = {
    'D1110D'        :  'Raw cows milk delivered to dairies',
    'D1200DME'      :  'Raw cream delivered to dairies (in milk equivalent)',
    'D2100'         :  'Drinking milk',
    'D2200V'        :  'Cream for direct consumption',
    'D3100_X_3113'  :  'Milk and cream powders, excluding skimmed milk powders',
    'D3113'         :  'Skimmed milk powder',
    'D3200'         :  'Concentrated milk',
    'D4100'         :  'Acidified milk (yoghurts and other)',
    'D6000'         :  'Butter, incl. dehydrated butter and ghee, and other fats and oils derived from milk; dairy spreads',
    'D7121'         :  'Cheese from cows milk (pure)'
}




############################################################################################################################################################################################

###Pega o inventário de suínos e bovinos
#-> Datailed Datasets 
#   -> Agriculture, forestry and fisheries
#      -> Agricultural production (apro)
#         -> Animal production (apro_anip)
#            -> Livestock and meat (apro_mt)
#               -> Livestock (apro_mt_p)
#                  -> Bovine population - annual data  /  Pig population - annual data (apro_mt_lspig)


def get_pig_inventory():
    gz_file_path = f'{destination_path}/EU_Pig_Population.tsv.gz'
    tsv_file = f'{destination_path}/EU_Pig_Population.tsv'

    url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/apro_mt_lspig?format=TSV&compressed=true'
    response = requests.get(url, proxies=proxies)
    with open(gz_file_path, 'wb') as f:
        f.write(response.content)

    #Extraindo os arquivos ".gz"
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    with gzip.open(gz_file_path, 'rb') as gz_file:
        with open(os.path.join(destination_path, 'EU_Pig_Population.tsv'), 'wb') as arquivo_extraido:
            shutil.copyfileobj(gz_file, arquivo_extraido)

    linhas_tsv = []

    with open(f'{tsv_file}', 'r') as f:
        tsvreader = csv.reader(f, delimiter='\t')  # separador de campos é o TABULATION

        for linha in tsvreader:
            linhas_tsv.append(linha)

    df = pandas.DataFrame(linhas_tsv)
    new_columns = df[0].str.split(',', expand=True)
    df = pandas.concat([new_columns,df], axis=1)
    df = df.drop(columns=0)
    df.columns = df.iloc[0]
    df = df.rename(columns={'geo\TIME_PERIOD' : 'Country'})
    df = df[1:]
    df['animals'] = df['animals'].map(pig_codes)
    df = df.drop(columns='unit') 
    df[['month']] = df[['month']].apply(lambda x: x.str.replace('M',''))

    df = pandas.melt(df, id_vars=['month', 'Country', 'animals'], value_name='Animals (Thousand Heads)')
    df['Date'] = df['month']+"/"+df[0]
    coluna_date = df.pop('Date')
    df.insert(0, 'Date', coluna_date)
    df = df.drop(columns='month')
    df = df.drop(columns=0)

    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': ','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(':','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': c','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': u','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': u','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(':u','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' p',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' b',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' n',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' e',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' d',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace('p',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace('u',''))
    df[['Date']] = df[['Date']].apply(lambda x: x.str.replace('05_06','06'))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(':',''))

    df.loc[df['Country'] == 'T', 'Country'] = "MT"
    df.loc[df['Country'] == 'E', 'Country'] = "ME"
    df.loc[df['Country'] == 'K', 'Country'] = "MK"

    df['COUNTRY_NAME'] = df['Country'].map(countries_codes)
    coluna_country = df.pop('COUNTRY_NAME')
    df.insert(2, 'COUNTRY_NAME', coluna_country)
    df.insert(3, 'ANIMAL_GROUP', 'Swine')
    df.columns = ['DATE', 'COUNTRY', 'COUNTRY_NAME', 'ANIMAL_GROUP', 'TYPE_ANIMAL', 'ANIMALS_THOUSAND_HEADS']
    df['ANIMALS_THOUSAND_HEADS'] = df['ANIMALS_THOUSAND_HEADS'].replace(' ','')
    df['ANIMALS_THOUSAND_HEADS'] = pandas.to_numeric(df['ANIMALS_THOUSAND_HEADS'])
    df['DATE'] = df['DATE'].replace(' ','')
    df['DATE'] = df['DATE'].replace('/','-')
    df['DATE'] = pandas.to_datetime(df['DATE']).dt.strftime("%Y-%m-%d")
    df.to_excel(f"{destination_path}/EU_Pig_Inventory.xlsx", index=False)
    return df

def get_cattle_inventory():
    gz_file_path = f'{destination_path}/EU_Cattle_Population.tsv.gz'
    tsv_file = f'{destination_path}/EU_Cattle_Population.tsv'

    url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/apro_mt_lscatl?format=TSV&compressed=true'
    response = requests.get(url, proxies=proxies)
    with open(gz_file_path, 'wb') as f:
        f.write(response.content)

    #Extraindo os arquivos ".gz"
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    with gzip.open(gz_file_path, 'rb') as gz_file:
        with open(os.path.join(destination_path, 'EU_Cattle_Population.tsv'), 'wb') as arquivo_extraido:
            shutil.copyfileobj(gz_file, arquivo_extraido)

    linhas_tsv = []

    with open(f'{tsv_file}', 'r') as f:
        tsvreader = csv.reader(f, delimiter='\t')  # separador de campos é o TABULATION

        for linha in tsvreader:
            linhas_tsv.append(linha)

    df = pandas.DataFrame(linhas_tsv)
    new_columns = df[0].str.split(',', expand=True)
    df = pandas.concat([new_columns,df], axis=1)
    df = df.drop(columns=0)
    df.columns = df.iloc[0]
    df = df.rename(columns={'geo\TIME_PERIOD' : 'Country'})
    df = df[1:]
    df['animals'] = df['animals'].map(bovine_codes)
    df = df.drop(columns='unit') 
    df[['month']] = df[['month']].apply(lambda x: x.str.replace('M',''))

    df = pandas.melt(df, id_vars=['month', 'Country', 'animals'], value_name='Animals (Thousand Heads)')
    df['Date'] = df['month']+"/"+df[0]
    coluna_date = df.pop('Date')
    df.insert(0, 'Date', coluna_date)
    df = df.drop(columns='month')
    df = df.drop(columns=0)

    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': ','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': c','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': @C','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace(': u','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].replace('e','')
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' p',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' b',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' n',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' e',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(' d',''))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace('p',''))
    df[['Date']] = df[['Date']].apply(lambda x: x.str.replace('05_06','06'))
    df[['Animals (Thousand Heads)']] = df[['Animals (Thousand Heads)']].apply(lambda x: x.str.replace(':',''))
    df.loc[df['Country'] == 'T', 'Country'] = "MT"
    df.loc[df['Country'] == 'E', 'Country'] = "ME"
    df.loc[df['Country'] == 'K', 'Country'] = "MK"


    df['COUNTRY_NAME'] = df['Country'].map(countries_codes)
    coluna_country = df.pop('COUNTRY_NAME')
    df.insert(2, 'COUNTRY_NAME', coluna_country)
    df.insert(3, 'ANIMAL_GROUP', 'Cattle')
    df.columns = ['DATE', 'COUNTRY', 'COUNTRY_NAME', 'ANIMAL_GROUP', 'TYPE_ANIMAL', 'ANIMALS_THOUSAND_HEADS']
    df['ANIMALS_THOUSAND_HEADS'] = df['ANIMALS_THOUSAND_HEADS'].replace(' ','')
    df['ANIMALS_THOUSAND_HEADS'] = pandas.to_numeric(df['ANIMALS_THOUSAND_HEADS'], errors='coerce')
    df['DATE'] = pandas.to_datetime(df['DATE']).dt.strftime("%Y-%m-%d")

    df.to_excel(f"{destination_path}/EU_Cattle_Inventory.xlsx", index=False)
    return df


############################################################################################################################################################################################

###Pega a produção de Poultry
#-> Datailed Datasets 
#   -> Agriculture, forestry and fisheries
#      -> Agricultural production (apro)
#         -> Animal production (apro_anip)
#            -> Poultry farming (apro_ec)
#               -> Poultry - monthly data (apro_ec_poulm)


def get_poultry_inventory():
    start = time.time()

    gz_file_path = f'{destination_path}/EU_Poultry_Production.tsv.gz'
    tsv_file = f'{destination_path}/EU_Poultry_Production.tsv'

    url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/apro_ec_poulm?format=TSV&compressed=true'
    response = requests.get(url, proxies=proxies)
    with open(gz_file_path, 'wb') as f:
        f.write(response.content)

    #Extraindo os arquivos ".gz"
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    with gzip.open(gz_file_path, 'rb') as gz_file:
        with open(os.path.join(destination_path, 'EU_Poultry_Production.tsv'), 'wb') as arquivo_extraido:
            shutil.copyfileobj(gz_file, arquivo_extraido)

    linhas_tsv = []

    with open(f'{tsv_file}', 'r') as f:
        tsvreader = csv.reader(f, delimiter='\t')  # separador de campos é o TABULATION

        for linha in tsvreader:
            linhas_tsv.append(linha)
            
    df = pandas.DataFrame(linhas_tsv)
    new_columns = df[0].str.split(',', expand=True)
    df = pandas.concat([new_columns,df], axis=1)
    df = df.drop(columns=0)
    df.columns = df.iloc[0]
    df = df.rename(columns={'geo\TIME_PERIOD' : 'Country'})
    df = df[1:]
    df['animals'] = df['animals'].map(poultry_codes)
    df['hatchitm'] = df['hatchitm'].map(poultry_hatch_item_codes)
    
    
    df = df.replace(': ','')
    df = df.replace(': c','')
    df = df.apply(lambda x: x.str.replace(' p',''))
    df = df.apply(lambda x: x.str.replace(' e',''))
    df = df.apply(lambda x: x.str.replace(' b',''))
    df = df.apply(lambda x: x.str.replace(' @C',''))
    df = df.apply(lambda x: x.str.replace(' c',''))
    df = df.apply(lambda x: x.str.replace('b',''))
    df = df.apply(lambda x: x.str.replace('p',''))
    df = df.apply(lambda x: x.str.replace(':c',''))
    df = df.apply(lambda x: x.str.replace('ME','E'))
    df = df.apply(lambda x: x.str.replace('MK','K'))
    df = df.apply(lambda x: x.str.replace('MT','T'))


    df_for_use = df.loc[df['hatchitm'] == 'Chicks for use']
    df_incubation = df.loc[df['hatchitm'] == 'Eggslaced in incuation'] #O nome fica assim pois estou removendo o ' p' nas linhas de cima, depois é só renomear


    df_for_use = df_for_use.drop(columns='hatchitm')
    df_for_use = df_for_use.drop(columns='unit')
    df_for_use = pandas.melt(df_for_use, id_vars=['Country', 'animals'], var_name='Month', value_name='Values')
    df_for_use['ANIMAL_GROUP'] = 'Chicks for use'
    df_for_use.columns = ['COUNTRY', 'TYPE_ANIMAL', 'DATE', 'ANIMALS_THOUSAND_HEADS', 'ANIMAL_GROUP']
    df_for_use['COUNTRY_NAME'] = df_for_use['COUNTRY'].map(countries_codes)
    df_for_use = df_for_use[['DATE', 'COUNTRY', 'COUNTRY_NAME','ANIMAL_GROUP', 'TYPE_ANIMAL', 'ANIMALS_THOUSAND_HEADS']]
    df_for_use['ANIMALS_THOUSAND_HEADS'] = pandas.to_numeric(df_for_use['ANIMALS_THOUSAND_HEADS'], errors='coerce')

    df_incubation = df_incubation.drop(columns='hatchitm')
    df_incubation = df_incubation.drop(columns='unit')
    df_incubation = pandas.melt(df_incubation, id_vars=['Country', 'animals'], var_name='Month', value_name='Values')
    df_incubation['ANIMAL_GROUP'] = 'Eggs placed in incubation'
    df_incubation.columns = ['COUNTRY', 'TYPE_ANIMAL', 'DATE', 'ANIMALS_THOUSAND_HEADS', 'ANIMAL_GROUP']
    df_incubation['COUNTRY_NAME'] = df_incubation['COUNTRY'].map(countries_codes)
    df_incubation = df_incubation[['DATE', 'COUNTRY', 'COUNTRY_NAME','ANIMAL_GROUP', 'TYPE_ANIMAL', 'ANIMALS_THOUSAND_HEADS']]
    df_incubation['ANIMALS_THOUSAND_HEADS'] = pandas.to_numeric(df_incubation['ANIMALS_THOUSAND_HEADS'], errors='coerce')

    df_poultry = pandas.concat([df_for_use, df_incubation], axis=0)
    
    df_poultry['DATE'] = pandas.to_datetime(df_poultry['DATE']).dt.strftime("%Y-%m-%d")
    df_poultry = df_poultry[df_poultry['DATE']>='2000-01-01']

    df_poultry.to_excel(f'{destination_path}/EU_Poultry_Production.xlsx', header=True, index=False)


  
    end = time.time()

    print(f'A execução do Poulttry Inventory levou: {(end-start)/60} segundos')
    return df_poultry

############################################################################################################################################################################################
