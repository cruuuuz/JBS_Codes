import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from datetime import date
import pandas
import functions_get_inventory as EU
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Codes')
from credentials import create_snowflake_connection, set_snowflake_context


def get_Livestock_Inventory():
    df_pig_inventory = EU.get_pig_inventory()
    df_cattle_inventory = EU.get_cattle_inventory()
    df_poultry_inventory = EU.get_poultry_inventory()

    return [
        df_pig_inventory, 
        df_cattle_inventory,
        df_poultry_inventory
        ]

def send_inventory(list_dfs: list):
    """
    Recebe uma lista de DataFrames contendo dados de preços e envia para o Snowflake.
    Faz o controle de atualização com base na data mais recente de cada produto.
    """
    jan_1_last_year = date(date.today().year - 1, 1, 1)

    cnn = create_snowflake_connection()
    cursor = cnn.cursor()
    set_snowflake_context(cursor, 'MID')

    animal_groups = ['Swine', 'Cattle', 'Chicks for use', 'Eggs placed in incubation']

    df_for_group = {
        'Swine'                     : list_dfs[0],
        'Cattle'                    : list_dfs[1],
        'Chicks for use'            : list_dfs[2],
        'Eggs placed in incubation' : list_dfs[2]
    }

    for i in range(len(animal_groups)):
        df_inventory_geral = df_for_group[animal_groups[i]]
        df_inventory_geral = df_inventory_geral[df_inventory_geral['DATE']>=jan_1_last_year.isoformat()]
        
        cursor.execute(f"DELETE FROM EUROPE_LIVESTOCK_INVENTORY WHERE ANIMAL_GROUP = '{animal_groups[i]}' AND DATE>= '{str(jan_1_last_year)}'")
        x = spd.write_pandas(cnn, df_inventory_geral, table_name="EUROPE_LIVESTOCK_INVENTORY")
        print(f"Data for {animal_groups[i]} updated")

