import pandas as pd
from sqlalchemy import create_engine
from systems_connector import SystemConnectors  # Assuming this is a valid import
from decouple import config
import pyodbc

class DataLoad:

    @staticmethod
    def loadToMSSQL(tablename, server, databasename, username, password, driver='SQL Server Native Client 11.0'):
        connection_string = SystemConnectors.WarehouseConnect(driver, server, databasename, username, password)

        getAPITables = SystemConnectors.D365Connector(tablename)


        df = pd.DataFrame(getAPITables)
        print('Dataframe Created Successfully')

        try:
            df.to_sql('Audits', con=connection_string, if_exists='replace', index=False)
            print(f'Loading Data Into Bronze Container Finished Without Errors')
        except Exception as e:
            print(f'Error : {e}')

# Define database connection parameters
driver = "SQL Server Native Client 11.0"
server = "DKARIUKI-PC\\BASESSQL_SERVER"
databasename = "Bronze"
username = 'sa'  # Use environment variable for security
password = 'Saf3rthanc0v1d19'  # Use environment variable for security

DataLoad.loadToMSSQL('new_caseauditses',
    server=server,
    databasename=databasename,
    username=username,
    password=password
)
# --jub_benefitdetail