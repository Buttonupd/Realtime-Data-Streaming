from flask import Flask, jsonify
from sqlalchemy import create_engine as engine
import urllib
import pandas as pd
from systems_connector import SystemConnectors

app = Flask(__name__)
# connectionstr = SystemConnectors.WarehouseConnect( 
    
    
#     server='DKARIUKI-PC\BASESSQL_SERVER',
#     database='Bronze',
#     uid='sa',
#     password='Saf3rthanc0v1d19'
# )

# Define your SQL Server connection parameters
connectionstr = urllib.parse.quote_plus('''
    driver={ODBC DRIVER 18 for SQL Server};
    server=dmwihiki-pc\BASESQL_SERVER;
    database=Bronze;
    TRUSTED_CONNECTION=yes;
    TrustServerCertificate=yes;
''')

# Establish connection to SQL Server
connection_engine = engine('mssql+pyodbc:///?odbc_connect={}'.format(connectionstr))

@app.route('/loans/', methods=['GET'])
def get_loans():
    try:
        # Execute the SQL query and fetch the results into a DataFrame
        query = 'SELECT TOP 10 * FROM Loans'
        df = pd.read_sql_query(sql=query, con=connection_engine)
        
        # Convert DataFrame rows to a list of dictionaries
        loans = []
        for index, row in df.iterrows():
            loan = {
                'Application Date': row['Application Date'],
                'Product': row['Loan Product Type']
                # Add more fields as needed
            }
            loans.append(loan)
        return jsonify(loans)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
# @app.route('/api/load', methods=['GET', 'POST'])
# def load():
#     query = 'INSERT INTO WhateverTable WHERE id'


if __name__ == '__main__':
    app.run(debug=True)
# CX TO Claims Date 

# Teams Filter

# Notes
# -- ALTER VIEW Base_Productivity_Report AS
# SELECT
#     COUNT(auds._new_case_value) AS [Case_Count],
#     auds.new_textsystemuser AS [Case_Owner],
#     auds.new_casetype AS [Case_Type],
#     inc.createdon AS [Case_Creation_Date],
#     inc_r.createdon AS [Case_Resolved_On],
#     DATEDIFF(DAY, inc.createdon, inc_r.createdon) AS E2E_TAT,
#     AVG(DATEDIFF(DAY, CAST(inc.createdon as datetime), CAST(inc_r.createdon as datetime))) As Average_TAT,
#     teams.name AS [Team_Name],
#     CASE   
#         WHEN teams.jub_region = '973570003' THEN 'Uganda'
#         WHEN teams.jub_region = '973570002' THEN 'Tanzania'
#         WHEN teams.jub_region = '973570001' THEN 'Kenya'
#         ELSE 'None'
#     END AS Country,

#     dis.jub_name AS [Disposition],
#     CASE 
#         WHEN inc.statecode=0 THEN 'Active'
#         WHEN inc.statecode=1 THEN 'Resolved'
#         WHEN inc.statecode=2 THEN 'Cancelled'
#     ELSE 'None'
#     END AS [CRM_Status],
#     bus.jub_name as [Business_Line]

# FROM
#     [Jubilee Audits] auds
#     INNER JOIN [Jubilee Incidents ] inc ON auds._new_case_value = inc.incidentid
#     LEFT JOIN [Jubilee Incidents Resolution] inc_r ON auds._new_case_value = inc_r._incidentid_value
#     INNER JOIN [Jubilee Team Members] mems ON inc._jub_teamname_value = auds._new_teamassigned_value
#     INNER JOIN [Jubilee Teams] teams ON mems.teamid = teams.teamid
#     INNER JOIN [Jubilee Disposition] dis ON inc._jub_disposition_value = dis.jub_dispositionid
#     INNER JOIN [Jubilee Business Line] bus ON inc._jub_businessline_value = bus.jub_businesslineid 

# WHERE auds.new_textsystemuser IS NOT NULL 
#     AND teams.name != 'Test Team'
#     AND teams.name != 'opportunity b0b69c21-53d7-ee11-904d-000d3ab6035b+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'unqecee3d92acd7ee119048000d3ab73'
#     AND teams.name != 'CustomerVoiceProjectOwnerTeam_eefa2a73-dc9a-4421-a7a6-124ce55874d7'
#     AND teams.name != 'opportunity dde05c27-57d7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity c6d44527-5fd7-ee11-904d-000d3ab6035b+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity c6d44527-5fd7-ee11-904d-000d3ab6035b+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity 4c96edd0-63d7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity dde05c27-57d7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity 4c96edd0-63d7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity 06d73481-5bd7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity 06d73481-5bd7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity 06d73481-5bd7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
#     AND teams.name != 'opportunity 06d73481-5bd7-ee11-904d-000d3a674348+b3228218-b8f6-4a1b-a18d-552c6f980831'
            

# AND new_casetype IS NOT NULL
# GROUP BY
#     auds.new_textsystemuser,
#     auds.new_casetype,
#     inc.createdon,
#     inc_r.createdon,
#     teams.name,
#     dis.jub_name,
#     inc.statecode,
#     bus.jub_name,
#     teams.jub_region
