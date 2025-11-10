import requests 
import pandas

class Beef:
    @staticmethod
    def get_Production(base_url: str, proxies: dict, date: str):
        endpoint = "/api/beef/production?"
        params = {
            "memberStateCode" : "true",
            "beginDate"  : "01/01/2017",
            "endDate"    : date,
            "unit"       : "tonnes"
        }

        response = requests.get(f"{base_url}{endpoint}", params=params, proxies=proxies)
        
        df_geral = pandas.DataFrame(response.json())

        # category = ['Adult male indicative price', 'Calves slaughtered <8M', 'Cows', 'Heifers', 'Steers', 'Young cattle']
        # df_geral = df_geral[df_geral['category'].isin(category)]

        df_geral["date"] = pandas.to_datetime(df_geral["year"].astype(str) + " " + df_geral["month"], format="%Y %B")
        df_geral = df_geral[['date', 'memberStateCode', 'memberStateName', 'category', 'tonnes', 'heads', 'kgPerHead']]
        df_geral.columns = ['REFERENCE_DATE', 'COUNTRY_CODE', 'COUNTRY_NAME', 'CATEGORY', 'PRODUCTION_THOUSAND_TONS', 'SLAUGHTER_THOUSAND_HEADS', 'KG_WEIGHT_PER_HEAD']

        df_geral['PRODUCTION_THOUSAND_TONS'] = pandas.to_numeric(df_geral['PRODUCTION_THOUSAND_TONS'])
        df_geral['SLAUGHTER_THOUSAND_HEADS'] = pandas.to_numeric(df_geral['SLAUGHTER_THOUSAND_HEADS'])
        df_geral['KG_WEIGHT_PER_HEAD'] = pandas.to_numeric(df_geral['KG_WEIGHT_PER_HEAD'])

        df_geral['COUNTRY_NAME'] = df_geral['COUNTRY_NAME'].str.upper()
        df_geral['CATEGORY'] = df_geral['CATEGORY'].str.upper()
        df_geral.insert(3, "ANIMAL_TYPE", "CATTLE/BEEF")


        return df_geral



