import pandas as pd 
from sqlalchemy import create_engine as engine 
import warnings 
import pyodbc 
import urllib
from decouple import config
import requests
from functools import lru_cache 

class SystemConnectors:

    # Oracle, MSSQL,API, Flat File, D365, BC, Call Center

    def __init__(self) -> None:
        pass 
    
    @lru_cache
    def WarehouseConnect(server, databasename, username, password, driver='SQL Server Native Client 11.0',) -> tuple: # Target System

        # Function to return the connection string to the central repository db
        # Bronze - Unprocessed  raw data 
        # 
        def return_ConnectionString():
            warehouse_properties = urllib.parse.quote(
                f'''
                    DRIVER={driver};
                    SERVER={server};
                    DATABASE={databasename}; 
                    UID={username};
                    PWD={password};
                ''')
            
            connection_engine = engine('mssql+pyodbc:///?odbc_connect={}'.format(warehouse_properties))
            return connection_engine
        
        connection_string = return_ConnectionString()

        available_engines = {
            'db_connector': connection_string
        }

        try:
            for conn, conn_value in available_engines.items():
                if conn_value.connect():
                    print(f'Successfully established a connection to {conn}')
        except:
            for conn, conn_value in available_engines.items():
                if not conn_value.connect():
                    print(f'Failed to establish a connection to {conn}')
        finally:
            for conn, conn_value in available_engines.items():
                conn_value.dispose()
                print(f'Successfully disposed off engine {conn}')
        
        return connection_string

    @lru_cache
    def D365Connector(tablename): 

        warnings.filterwarnings('ignore')
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        
        api_url = config('RESOURCE') + config('CE_ENDPOINT') + tablename
        # api_url = 'https://onejubileecrm.crm4.dynamics.com/api/data/v9.2/new_caseauditses' #jub_subdispositions
        client_id = config('CLIENT_ID')
        client_value = config('CLIENT_VALUE')
        tenant_id = config('TENANT_ID')
        resource = config('RESOURCE')

        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_value,
            'resource': resource
        }
        token_response = requests.post(token_url, data=token_data)
        access_token = token_response.json()

        # access_token = access_token['access_token']
        #print("Access :", access_token)
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json; charset=utf-8',
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
        }

        if token_response.status_code == 200:
            token_data = token_response.json()
            access_token = token_data.get('access_token')

            if access_token:
                headers['Authorization'] = f'Bearer {access_token}'
            else:
                print("Error : 'access_token' not found in token response")
        else:
            print("Error obtaining access token : ", token_response.status_code, token_response.text )
        
        all_records = [] 
        next_link = api_url

        print('Next Link',next_link)
        while True:
            response = requests.get(next_link, headers=headers)

            if response.status_code == 200:
                data = response.json()
                print(data)
                all_records.extend(data['value'])
                # print(all_records[0])
                if '@odata.nextLink' not in data:
                    break
                next_link = data['@odata.nextLink']
            else:
                print(response.text)
                break

        print(f"Total records retrieved: {len(all_records)}")

        df = pd.DataFrame(all_records)
        print(f'All record stored in Data Frame object')
        print(df.head(5))
        return df
    
# driver = "SQL Server Native Client 11.0"
# server = "DKARIUKI-PC\BASESSQL_SERVER"
# databasename = "Bronze"
# username = 'sa'  # Use environment variable for security
# password = 'Saf3rthanc0v1d19'  # Use environment variable for security

# SystemConnectors.WarehouseConnect(
#     server,
#     databasename,
#     username,
#     password
# )

        
            
    





              


