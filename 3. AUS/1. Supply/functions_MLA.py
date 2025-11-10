# #Indicadores usáveis
# -> Australian Livestock Indicators (/report/5)
# -> Australian Feedlot Cattle on Feed (manual) -> ok
# -> Australian Feedlot Turn-Off (manual)
# -> Australian Meat Exports (/report/1)
# -> Australian Over The Hooks Prices Cattle (/report/2)
# -> Australia Saleyard Cattle Transactions (/report/4)
# -> Australian Cattle Yardings (/report/4)


from datetime import datetime, timedelta
import requests
import sys
import pandas
import os
import snowflake.connector.pandas_tools as spd
sys.path.append('O:/Codes')
from credentials import create_snowflake_connection, set_snowflake_context, proxy_request
from dotenv import load_dotenv


proxies = proxy_request()

cnn = create_snowflake_connection()
cursor = cnn.cursor()
set_snowflake_context(cursor, 'MID')


def Get_Weekly_Slaughter():
    class MWCToken:
        mwc_token: str = None
        embed_token: str = None

        def __init__(self):
            self.embed_url = "https://app.nlrsreports.mla.com.au/statistics/nlrs-slaughter/getembedinfo"
            self.mwc_url = "https://wabi-australia-southeast-redirect.analysis.windows.net/explore/reports/fb18e389-c75c-4cbb-ae89-2a9447d9da50/modelsAndExploration?preferReadOnlySession=true&skipQueryData=true"

        def request_embed_token(self) -> str:
            response = requests.get(self.embed_url, proxies=proxies).json()
            mwc_token = response.get("accessToken", "")
            return mwc_token

        def get_embed_token(self) -> str:
            if not self.embed_token:
                self.embed_token = self.request_embed_token()
            return self.embed_token

        def request_mwc_token(self) -> str:
            header = {"Authorization": "EmbedToken "+self.get_embed_token()}
            response = requests.get(self.mwc_url, headers=header, proxies=proxies, verify=False).json()
            mwc_token = response.get("exploration", {}).get("mwcToken", "")
            return mwc_token

        def get_mwc_token(self) -> str:
            if not self.mwc_token:
                self.mwc_token = self.request_mwc_token()
            return self.mwc_token


    class MLAAustralia:
        def __init__(self):
            self.url = "https://93d14387498c4eda805c6ce6699c677b.pbidedicated.windows.net/webapi/capacities/93D14387-498C-4EDA-805C-6CE6699C677B/workloads/QES/QueryExecutionService/automatic/public/query"
            self.date_format = "%Y-%m-%dT%H:%M:%S"
            self.token_transport = MWCToken()

        def body(self, start: datetime, end: datetime):
            return {
                "version": "1.0.0",
                "queries": [{"Query": {"Commands": [{"SemanticQueryDataShapeCommand": {"Query": {
                    "Version": 2,
                    "From": [
                        {"Name": "s", "Entity": "Slaughter", "Type": 0},
                        {"Name": "c", "Entity": "Calendar", "Type": 0},
                        {"Name": "t", "Entity": "Time Frame", "Type": 0}
                    ],
                    "Select": [
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "s"}},
                                "Property": "Contributor State"
                            },
                            "Name": "Slaughter.Contributor State",
                            "NativeReferenceName": "State"
                        },
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "s"}},
                                "Property": "Species"
                            },
                            "Name": "Slaughter.Species",
                            "NativeReferenceName": "Species"
                        },
                        {
                            "Aggregation": {
                                "Expression": {"Column": {
                                    "Expression": {"SourceRef": {"Source": "s"}},
                                    "Property": "Slaughter Count"
                                }},
                                "Function": 0
                            },
                            "Name": "Sum(Slaughter.Slaughter Count)",
                            "NativeReferenceName": "Total Head"
                        },
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "c"}},
                                "Property": "Week End Date"
                            },
                            "Name": "Calendar.Week End Date",
                            "NativeReferenceName": "Weekly"
                        }
                    ],
                    "Where": [
                        {
                            "Condition": {"In": {
                                "Expressions": [{"Column": {
                                    "Expression": {"SourceRef": {"Source": "t"}},
                                    "Property": "Time Frame Fields"
                                }}],
                                "Values": [[{"Literal": {"Value": "'''Calendar''[Week End Date]'"}}]]
                            }}
                        },
                        {
                            "Condition": {"And": {
                                "Left": {
                                    "Comparison": {
                                        "ComparisonKind": 2,
                                        "Left": {"Column": {
                                            "Expression": {"SourceRef": {"Source": "c"}},
                                            "Property": "Date"
                                        }},
                                        "Right": {"Literal": {
                                            "Value": "datetime'"+start.strftime(self.date_format) + "'"
                                        }}
                                    }
                                },
                                "Right": {
                                    "Comparison": {
                                        "ComparisonKind": 3,
                                        "Left": {"Column": {
                                            "Expression": {"SourceRef": {"Source": "c"}},
                                            "Property": "Date"
                                        }},
                                        "Right": {"Literal": {
                                            "Value": "datetime'"+end.strftime(self.date_format) + "'"
                                        }}
                                    }
                                }
                            }}
                        },
                        {
                            "Condition": {"In": {
                                "Expressions": [{"Column": {
                                    "Expression": {"SourceRef": {"Source": "s"}},
                                    "Property": "Species"
                                }}],
                                "Values": [
                                    [{"Literal": {"Value": "'Cattle'"}}],
                                    [{"Literal": {"Value": "'Lambs'"}}],
                                    [{"Literal": {"Value": "'Sheep'"}}]
                                ]
                            }}
                        },
                        {
                            "Condition": {"Not": {"Expression": {"In": {
                                "Expressions": [{"Column": {
                                    "Expression": {"SourceRef": {"Source": "s"}},
                                    "Property": "Contributor State"
                                }}],
                                "Values": [[{"Literal": {"Value": "'ES'"}}]]
                            }}}}
                        }
                    ]
                }, "Binding": {
                "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3]}]},
                "DataReduction": {"DataVolume": 3},
                "Version": 1
                }}}]}}],
                "cancelQueries": [],
                "modelId": 4014768,
                "userPreferredLocale": "en",
                "allowLongRunningQueries": True
            }

        @staticmethod
        def parse_response(response: dict):
            result = response["results"][0]["result"]["data"]["dsr"]["DS"][0]
            datamatrix = result["PH"][0]["DM0"]
            states, species = result['ValueDicts'].values()
            organized_data = []
            for row in datamatrix:
                row_data = row.get("C")
                try:
                    date, slaughter = datetime.fromtimestamp(row_data[-2]//1000), row_data[-1]
                    if len(row_data) == 4:
                        state, specie = states[row_data[0]], species[row_data[1]]
                    else:
                        repeat = row.get("R")
                        if repeat == 3:
                            state, specie = organized_data[-1][:2]
                        elif repeat != 1:
                            raise ValueError("Unexpected behaviour")
                        else:
                            state, specie = organized_data[-1][0], species[row_data[0]]
                    organized_data.append([state, specie, date, slaughter])
                except Exception as e:
                    print(f'Error: {e}, because the input data_row was: {row_data}')
                    pass
            return organized_data

        def get(self, start: datetime, end: datetime):
            mwc_token = self.token_transport.get_mwc_token()
            headers = {"Authorization": "MWCToken "+mwc_token}
            body = self.body(start, end)
            response = requests.post(self.url, json=body, headers=headers, proxies=proxies, verify=False)
            parsed_response = self.parse_response(response.json())
            return parsed_response

    transport = MLAAustralia()
    today = datetime.now()
    six_months_ago = today - timedelta(days=180)
    resp = transport.get(six_months_ago, today)
    resp = pandas.DataFrame(resp)
    resp.columns = ['STATE', 'SPECIE', 'WEEK_ENDING_DATE', 'VALUE']
    return resp

def send_weekly_slaughter():
    df_weekly_slaughter = pandas.DataFrame(Get_Weekly_Slaughter())
    df_weekly_slaughter = df_weekly_slaughter[['WEEK_ENDING_DATE', 'STATE', 'SPECIE', 'VALUE']]
    df_weekly_slaughter['WEEK_ENDING_DATE'] = pandas.to_datetime(df_weekly_slaughter['WEEK_ENDING_DATE'])
    df_weekly_slaughter['WEEK_ENDING_DATE'] = df_weekly_slaughter['WEEK_ENDING_DATE'] - timedelta(days=1)
    df_weekly_slaughter['WEEK_ENDING_DATE'] = df_weekly_slaughter['WEEK_ENDING_DATE'].dt.strftime("%Y-%m-%d")
    df_weekly_slaughter = df_weekly_slaughter[df_weekly_slaughter['SPECIE'] == 'Cattle']

    last_date = df_weekly_slaughter['WEEK_ENDING_DATE'].max()
    last_date_snowflake = cursor.execute(f"SELECT MAX(WEEK_ENDING_DATE) FROM AUS_WEEKLY_SLAUGHTER")
    result_date = cursor.fetchall()
    date_snowflake = str(result_date[0][0])
    consulta_last_date = cursor.execute(f"SELECT * FROM AUS_WEEKLY_SLAUGHTER WHERE WEEK_ENDING_DATE = '{last_date}'") 
    results = cursor.fetchall()
    if results==[]:
        df_to_snowflake = df_weekly_slaughter.loc[df_weekly_slaughter['WEEK_ENDING_DATE'] > date_snowflake]
        x = spd.write_pandas(cnn, df_to_snowflake, table_name="AUS_WEEKLY_SLAUGHTER")
        print(f'Data about Weekly Slaughter at date {last_date} was updated')
    else:
        print(f'Data about Weekly Slaughter at date {last_date} already exists')



def Get_Cattle_On_Feeed():
    datas = pandas.date_range('1900-01-01', '2100-12-31', freq='Q')
    lista_datas = datas.tolist()

    today = datetime.now()
    start_date = "2010-01-01"
    end_date = today

    class MWCToken:
        mwc_token: str = None
        embed_token: str = None

        def __init__(self):
            self.embed_url = "https://app.nlrsreports.mla.com.au/statistics/aus-feedlot-cattle-on-feed/getembedinfo"
            self.mwc_url = "https://wabi-australia-southeast-redirect.analysis.windows.net/explore/reports/11a2783f-80db-4c14-a737-24c01d2db8bf/modelsAndExploration?preferReadOnlySession=true&skipQueryData=true"

        def request_embed_token(self) -> str:
            response = requests.get(self.embed_url, proxies=proxies).json()
            mwc_token = response.get("accessToken", "")
            return mwc_token

        def get_embed_token(self) -> str:
            if not self.embed_token:
                self.embed_token = self.request_embed_token()
            return self.embed_token

        def request_mwc_token(self) -> str:
            header = {"Authorization": "EmbedToken "+self.get_embed_token()}
            response = requests.get(self.mwc_url, headers=header, proxies=proxies, verify=False).json()
            mwc_token = response.get("exploration", {}).get("mwcToken", "")
            return mwc_token

        def get_mwc_token(self) -> str:
            if not self.mwc_token:
                self.mwc_token = self.request_mwc_token()
            return self.mwc_token


    class MLAAustralia:
        def __init__(self):
            self.url = "https://93d14387498c4eda805c6ce6699c677b.pbidedicated.windows.net/webapi/capacities/93D14387-498C-4EDA-805C-6CE6699C677B/workloads/QES/QueryExecutionService/automatic/public/query"
            self.date_format = "%Y-%m-%dT%H:%M:%S"
            self.token_transport = MWCToken()

        def body(self, start: datetime, end: datetime):
            return {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"c2","Entity":"Cattle on Feed","Type":0},{"Name":"c","Entity":"Calendar","Type":0},{"Name":"l","Entity":"Locations","Type":0}],"Select":[{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"c2"}},"Property":"Head Count"}},"Function":0},"Name":"Sum(Cattle on Feed.Head Count)"},{"Column":{"Expression":{"SourceRef":{"Source":"c2"}},"Property":"Capacity Description"},"Name":"Cattle on Feed.Capacity Description"},{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Quarter End Date"},"Name":"Calendar.Quarter End Date","NativeReferenceName":"Quarter End Date"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Location Description"},"Name":"Locations.Location Description","NativeReferenceName":"Location"}],"Where":[{"Condition":{"Not":{"Expression":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Quarter End Date"}}],"Values":[[{"Literal":{"Value":"null"}}]]}}}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Location Description"}}],"Values":[[{"Literal":{"Value":"'Australia'"}}]]}}},{"Condition":{"And":{"Left":{"Comparison":{"ComparisonKind":2,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Date"}},"Right":{"Literal":{"Value":"datetime'"+start+"T00:00:00'"}}}},"Right":{"Comparison":{"ComparisonKind":3,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Date"}},"Right":{"Literal":{"Value":"datetime'"+end.strftime("%Y-%m-%d")+"T00:00:00'"}}}}}}}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Quarter End Date"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[2]},{"Projections":[3]}]},"Secondary":{"Groupings":[{"Projections":[0,1]}]},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":100}},"Secondary":{"Top":{"Count":100}}},"Version":1},"ExecutionMetricsKind":1}}]},"QueryId":"","ApplicationContext":{"DatasetId":"7580f63c-ac04-46d9-9507-08226d662d4b","Sources":[{"ReportId":"11a2783f-80db-4c14-a737-24c01d2db8bf","VisualId":"71d951329cd33af3412b"}]}}],"cancelQueries":[],"modelId":3492070,"userPreferredLocale":"pt-BR","allowLongRunningQueries":True}

        @staticmethod
        def parse_response(response: dict):
            result = response["results"][0]["result"]["data"]["dsr"]["DS"][0]
            datamatrix = result["PH"][0]["DM0"]
            location = result['ValueDicts'].values()
            organized_data = []
            position_date = lista_datas.index(pandas.to_datetime("2010-03-31")) #Voce mexe aqui quando bater o limite de 100 linhas na requisição do API
            for row in datamatrix:
                date_for_item = lista_datas[position_date]
                row_data = row.get("M")
                row_data = row_data[0]["DM1"][0].get("X")
                class_desc = ['<500', '>10000', '1000-10000', '500-1000', 'Total']
                for c in range(len(row_data)):
                    row_data[c] = {class_desc[c] : row_data[c].get('M0')}
                df = pandas.DataFrame(row_data)
                df = df.melt()
                df = df.dropna()
                df.columns = ['CAPACITY', 'VALUE']
                df['QUARTER_END_DATE'] = date_for_item
                df['COUNTRY'] = 'AUSTRALIA'
                df = df[['QUARTER_END_DATE', 'COUNTRY', 'CAPACITY', 'VALUE']]
                organized_data.append(df)
                position_date += 1
            return organized_data

        def get(self, start: datetime, end: datetime):
            mwc_token = self.token_transport.get_mwc_token()
            headers = {"Authorization": "MWCToken "+mwc_token}
            body = self.body(start, end)
            response = requests.post(self.url, json=body, headers=headers, proxies=proxies, verify=False)
            parsed_response = self.parse_response(response.json())
            return parsed_response

    transport = MLAAustralia()
    resp = transport.get(start_date, end_date)
    return resp

def send_cattle_on_feed():
    df_cof = pandas.concat(Get_Cattle_On_Feeed())
    last_date = df_cof['QUARTER_END_DATE'].max()
    last_date_snowflake = cursor.execute(f"SELECT MAX(QUARTER_END_DATE) FROM AUS_CATTLE_ON_FEED")
    result_date = cursor.fetchall()
    date_snowflake = str(result_date[0][0])
    consulta_last_date = cursor.execute(f"SELECT * FROM AUS_CATTLE_ON_FEED WHERE QUARTER_END_DATE = '{last_date}'") 
    results = cursor.fetchall()
    if results==[]:
        df_to_snowflake = df_cof.loc[df_cof['QUARTER_END_DATE'] > date_snowflake]
        df_to_snowflake['QUARTER_END_DATE'] = pandas.to_datetime(df_to_snowflake['QUARTER_END_DATE']).dt.strftime("%Y-%m-%d")
        x = spd.write_pandas(cnn, df_to_snowflake, table_name="AUS_CATTLE_ON_FEED")
        print(f'Data about Cattle on Feed at date {last_date} was updated')
    else:
        print(f'Data about Cattle on Feed at date {last_date} already exists')



def Get_Slaughter_by_Specie():
    datas = pandas.date_range('1900-01-01', '2100-12-31', freq='Q')
    lista_datas = datas.tolist()

    today = datetime.now()
    start_date = "2000-01-01"
    end_date = today

    class MWCToken:
        mwc_token: str = None
        embed_token: str = None

        def __init__(self):
            self.embed_url = "https://app.nlrsreports.mla.com.au/statistics/aus-production-slaughter/getembedinfo"
            self.mwc_url = "https://wabi-australia-southeast-redirect.analysis.windows.net/explore/reports/62905756-5040-4d81-bc8c-dbb7e1329444/modelsAndExploration?preferReadOnlySession=true&skipQueryData=true"

        def request_embed_token(self) -> str:
            response = requests.get(self.embed_url, proxies=proxies).json()
            mwc_token = response.get("accessToken", "")
            return mwc_token

        def get_embed_token(self) -> str:
            if not self.embed_token:
                self.embed_token = self.request_embed_token()
            return self.embed_token

        def request_mwc_token(self) -> str:
            header = {"Authorization": "EmbedToken "+self.get_embed_token()}
            response = requests.get(self.mwc_url, headers=header, proxies=proxies, verify=False).json()
            mwc_token = response.get("exploration", {}).get("mwcToken", "")
            return mwc_token

        def get_mwc_token(self) -> str:
            if not self.mwc_token:
                self.mwc_token = self.request_mwc_token()
            return self.mwc_token


    class MLAAustralia:
        def __init__(self):
            self.url = "https://93d14387498c4eda805c6ce6699c677b.pbidedicated.windows.net/webapi/capacities/93D14387-498C-4EDA-805C-6CE6699C677B/workloads/QES/QueryExecutionService/automatic/public/query"
            self.date_format = "%Y-%m-%dT%H:%M:%S"
            self.token_transport = MWCToken()

        def body(self, start: datetime, end: datetime):
            return {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"a","Entity":"ABS Production & Slaughter","Type":0},{"Name":"c","Entity":"Calendar","Type":0},{"Name":"t","Entity":"Time Frame","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Livestock Category"},"Name":"ABS Production & Slaughter.Livestock Category"},{"Measure":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Value Amt"},"Name":"ABS Production & Slaughter.Value Amt"},{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Quarter End Date"},"Name":"Calendar.Quarter End Date","NativeReferenceName":"Quarterly"}],"Where":[{"Condition":{"Comparison":{"ComparisonKind":1,"Left":{"Measure":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Value Amt"}},"Right":{"Literal":{"Value":"0L"}}}},"Target":[{"Column":{"Expression":{"SourceRef":{"Source":"c"}},"Property":"Quarter End Date"}},{"Column":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Livestock Category"}}]},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Report Type"}}],"Values":[[{"Literal":{"Value":"'Slaughter'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Species Description"}}],"Values":[[{"Literal":{"Value":"'Cattle'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"a"}},"Property":"Location"}}],"Values":[[{"Literal":{"Value":"'Australia'"}}]]}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"Time Frame Fields"}}],"Values":[[{"Literal":{"Value":"'''Calendar''[Quarter End Date]'"}}]]}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[1,2]}]},"Secondary":{"Groupings":[{"Projections":[0]}]},"DataReduction":{"DataVolume":4,"Intersection":{"BinnedLineSample":{}}},"Version":1},"ExecutionMetricsKind":1}}]},"QueryId":"","ApplicationContext":{"DatasetId":"2eb0f986-6ab4-4b02-a201-9350691b91c8","Sources":[{"ReportId":"62905756-5040-4d81-bc8c-dbb7e1329444","VisualId":"f3485b366b11a9be61da"}]}}],"cancelQueries":[],"modelId":3491795,"userPreferredLocale":"pt-BR","allowLongRunningQueries":True}

        @staticmethod
        def parse_response(response: dict):
            result = response["results"][0]["result"]["data"]["dsr"]["DS"][0]
            datamatrix = result["PH"][0]["DM0"]
            species = []
            for i in range(0,len(result['SH'][0]['DM1'])):
                species.append(result['SH'][0]['DM1'][i]['G1'])
            organized_data = []
            position_date = lista_datas.index(pandas.to_datetime("2000-03-31"))
            for row in datamatrix:
                date_for_item = lista_datas[position_date]
                row_data = row.get("X")
                for i in range(1,len(row_data)):
                    row_data[i] = {species[i] : row_data[i].get('M0')}
                df = pandas.DataFrame(row_data)
                df = df.melt()
                df = df.dropna()
                df.columns = ['SPECIE', 'VALUE']
                df['QUARTER_END_DATE'] = date_for_item
                df['REPORT_TYPE'] = 'SLAUGHTER'
                df['LOCATION'] = 'AUSTRALIA'
                df['UNIT'] = 'Head'
                df = df[['QUARTER_END_DATE', 'REPORT_TYPE', 'LOCATION', 'SPECIE', 'VALUE', 'UNIT']]
                organized_data.append(df)
                position_date += 1
            return organized_data

        def get(self, start: datetime, end: datetime):
            mwc_token = self.token_transport.get_mwc_token()
            headers = {"Authorization": "MWCToken "+mwc_token}
            body = self.body(start, end)
            response = requests.post(self.url, json=body, headers=headers, proxies=proxies, verify=False)
            parsed_response = self.parse_response(response.json())
            return parsed_response

    transport = MLAAustralia()
    resp = transport.get(start_date, end_date)
    return resp

def send_quarterly_slaughter():
    df_slaughter = pandas.concat(Get_Slaughter_by_Specie())
    species = ['Bobby Calves', 'Bulls, Bullocks And Steers', 'Calves', 'Cattle (Excl. Calves)', 'Cows And Heifers', 'Other Calves']
    # df_slaughter['SPECIE'] = df_slaughter['SPECIE'].replace('Cattle (Excl. Calves)', 'Cows And Heifers')
    # df_slaughter['SPECIE'] = df_slaughter['SPECIE'].replace('Calves', 'Cattle (Excl. Calves)')
    # df_slaughter['SPECIE'] = df_slaughter['SPECIE'].replace('Bulls, Bullocks And Steers', 'Calves')
    # df_slaughter['SPECIE'] = df_slaughter['SPECIE'].replace('M0','Bulls, Bullocks And Steers')
    df_slaughter = df_slaughter[df_slaughter['SPECIE'].isin(species)]
    df_slaughter = df_slaughter.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    df_slaughter['VALUE'] = df_slaughter['VALUE'].str.replace('D','')
    df_slaughter['VALUE'] = pandas.to_numeric(df_slaughter['VALUE'])
    last_date = df_slaughter['QUARTER_END_DATE'].max()
    last_date_snowflake = cursor.execute(f"SELECT MAX(QUARTER_END_DATE) FROM AUS_QUARTERLY_SLAUGHTER")
    result_date = cursor.fetchall()
    date_snowflake = str(result_date[0][0])
    consulta_last_date = cursor.execute(f"SELECT * FROM AUS_QUARTERLY_SLAUGHTER WHERE QUARTER_END_DATE = '{last_date}'") 
    results = cursor.fetchall()
    if results==[]:
        df_to_snowflake = df_slaughter.loc[df_slaughter['QUARTER_END_DATE'] > date_snowflake]
        df_to_snowflake['QUARTER_END_DATE'] = pandas.to_datetime(df_to_snowflake['QUARTER_END_DATE']).dt.strftime("%Y-%m-%d")
        x = spd.write_pandas(cnn, df_to_snowflake, table_name="AUS_QUARTERLY_SLAUGHTER")
        print(f'Data about Quarterly Slaughter at date {last_date} was updated')
    else:
        print(f'Data about Quarterly Slaughter at date {last_date} already exists')