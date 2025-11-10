import pandas
import sys
import asyncio


class United_States():
    @staticmethod    
    def get_Prices(path: str, ams_path: str):
        try:
            sys.path.append(f'{ams_path}')
            from ConectaAPI_AMS_USDA import AmsUsda
            
            date_base = "01/01/2010"
            current_period = 'WEEKLY WEIGHTED AVERAGES'
            selling_basis_description = 'Live'
            class_description = 'Steer'

            class CattlePrices:
                AMS_USDA : AmsUsda = AmsUsda()

                def __init__(self):
                    (
                        self.lm_ct150
                    ) = asyncio.run(self.build())
                
                async def build(self):
                    await self.AMS_USDA.instance_ids()
                    return await asyncio.gather(
                        self.calc_lm_ct150()
                    )

                async def calc_lm_ct150(self):
                    report_name = 'LM_CT150'
                    report_section = 3

                    function = await self.AMS_USDA.get_AMS_Cattle_data(report_name, report_section)
                    function = pandas.DataFrame(function['results'])
                    function = function.iloc[:, [0, 5, 6, 7, 10]]
                    function.columns = ['Dates', 'Periodo', 'Base', 'Classe', 'Average']
                    function = function.query(f'Dates>"{date_base}" and Periodo=="{current_period}" and Base=="{selling_basis_description}" and Classe=="{class_description}"')
                    return function.head(1000).to_csv(f'{path}/USDA.csv', sep = ';', index=False)

            CattlePrices().calc_lm_ct150()
            print('USA data downloaded')
            df_usa = pandas.read_csv(f'{path}/USDA.csv', sep = ';') #O : significa selecionar todas as linhas ou colunas, depende de onde você coloca
            df_usa = df_usa.iloc[:, [0, 4]] #O : significa selecionar todas as linhas ou colunas, depende de onde você coloca
            df_usa.to_excel(f'{path}/Preços_EUA.xlsx', index=False)
        except Exception as e:
            print(f'Error downloading USA prices: {e}')