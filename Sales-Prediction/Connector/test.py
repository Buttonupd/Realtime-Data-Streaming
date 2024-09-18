

# import pandas as pd
# import pyodbc
# from sqlalchemy import create_engine

# class ExtractTransform:

#     def __init__(self) -> None:
#         pass

#     @staticmethod
#     def tables_To_Df():
#         pd.set_option('display.max_columns', None)

#         connection_string = SystemConnectors.WarehouseConnect(
#             server,
#             databasename,
#             username,
#             password
#         )
#         query = '''
#             SELECT
#                 [Entry No_],
#                 [Posting Date],
#                 [Due Date],
#                 [Closed at Date],
#                 [Closed by Amount],
#                 [Document Date]
#             FROM [UNDT_SACCO_Staging].[dbo].Vendor_LedgerEntry
#             WHERE [Closed by Amount] > 0
#         '''
        
#         data = pd.read_sql_query(sql=query, con=connection_string)
#         return data

#     @staticmethod
#     def transform_columns(df):
#         # Transform column names to replace spaces with underscores
#         df.columns = [col.replace(' ', '_') for col in df.columns]
#         return df

#     @staticmethod
#     def write_to_sql(df, table_name, connection_string):
#         # Use SQLAlchemy to write the DataFrame to the database
#         engine = create_engine(connection_string)
#         df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

#     @staticmethod
#     def read_from_sql(table_name, connection_string):
#         # Read the new table from the database
#         query = f'SELECT * FROM {table_name}'
#         data = pd.read_sql_query(sql=query, con=connection_string)
#         return data

# # Example usage

# new_table_name = 'Transformed_Vendor_LedgerEntry'

# # Instantiate the class
# et = ExtractTransform()

# # Fetch data
# df = et.tables_To_Df()

# # Transform columns
# transformed_df = et.transform_columns(df)

# # Connection string for SQLAlchemy
# alchemy_connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{databasename}?driver=SQL+Server'

# # Write transformed DataFrame to new table
# et.write_to_sql(transformed_df, new_table_name, alchemy_connection_string)

# # Read the new table
# new_df = et.read_from_sql(new_table_name, alchemy_connection_string)
# print(new_df)





            
# # SELECT 
# #     CAST(inc.createdon AS datetime) as Case_Created_On,
# #     CAST(inc_r.createdon AS datetime) as Case_Resolved_On,
# #     inc.title,
# #     inc.ticketnumber,
# #     CASE 
# #         WHEN inc.jub_voucherstatus='973570000' THEN 'paid'
# #     ELSE 'None'
# #     END AS Voucher_Status,
# #     inc.incidentid,
# #     inc._ownerid_value,
# #     inc.jub_casematuritydate,
    
# #     CASE 
# #         WHEN inc.statecode=0 THEN 'Active'
# #         WHEN inc.statecode=1 THEN 'Resolved'
# #         WHEN inc.statecode=2 THEN 'Cancelled'
# #     ELSE 'None'
# #     END AS Statecode,
# #     CASE 
# #         WHEN inc.caseorigincode='1' THEN 'Phone'
# #         WHEN inc.caseorigincode='2' THEN 'Email'
# #         WHEN inc.caseorigincode='973570000' THEN 'Whatsapp'
# #         WHEN inc.caseorigincode='973570001' THEN 'Walk-In'
# #     ELSE 'None'
# #     END AS Case_Origin,
# #     CASE 
# #         WHEN inc.jub_casecreationtype='973570000' THEN 'Manual'
# #     ELSE 'None'
# #     END AS Case_Creation_Type,
# #     inc.jub_claimamount,
# #     CASE 
# #         WHEN inc.casetypecode=1 THEN 'Complaint'
# #         WHEN inc.casetypecode=2 THEN 'Service'
# #         WHEN inc.casetypecode=4 THEN 'Others'
# #         WHEN inc.casetypecode=5 THEN 'Life Claim'
# #         WHEN inc.casetypecode=6 THEN 'Pension Claim'
# #     ELSE 'None'
# #     END AS Case_Type,
# #     inc.jub_dispositionslife,
# #     inc.jub_dispositionspension,
# #     inc.jub_dateassigned,
# #     inc._jub_businessline_value,
# #     inc.jub_tatverification,
# #     inc.jub_vouchernumber,
# #     inc._owningteam_value,
# #     inc.jub_tatofaupload,
# #     inc.jub_tatvouchercreation,
# #     inc.jub_tatapprova,
# #     inc.jub_dateprocessed,
# #     inc.jub_ofapaymentstatus,
# #     CASE
# #         WHEN inc.jub_claimtype='973570000' THEN 'Maturity Claim'
# #         WHEN inc.jub_claimtype='973570001' THEN 'Death Claim'
# #         WHEN inc.jub_claimtype='973570003' THEN 'Indemnity Claim'
# #         WHEN inc.jub_claimtype='973570005' THEN 'Last Expense Claim'
# #         WHEN inc.jub_claimtype='973570006' THEN 'Loan Claim'
# #         WHEN inc.jub_claimtype='973570007' THEN 'Surrender Claim'
# #         WHEN inc.jub_claimtype='973570008' THEN 'Installment Claim'
# #         WHEN inc.jub_claimtype='973570009' THEN 'Refund Claim'
# #     ELSE 'NULL'
# #     END AS Claim_Type,
# #     inc.jub_ofauploadstatus,
# #     inc.jub_claimno,
# #     inc.jub_policyno,
# #     inc._jub_teamname_value,
# #     CAST(inc.jub_dateuploaded AS datetime) AS [OFA Date],
# #     CASE 
# #         WHEN inc.jub_region='973570001' THEN 'Kenya'
# #         WHEN inc.jub_region='973570002' THEN 'Uganda'
# #         WHEN inc.jub_region='973570003' THEN 'Tanzania'
# #     ELSE 'None'
# #     END AS Jubilee_Region,
# #     inc.jub_claimstatus,
# #     bus.jub_name as Entity,
# #     teams.name as Team,
# #     users.fullname as Owner,
# #     (SELECT COUNT(1) FROM [Jubilee Team Members] WHERE [Jubilee Team Members].systemuserid =inc._ownerid_value ) as TeamCount

# # FROM [Claims_TAT_BASE] inc 
# #     left JOIN [CRM_incident_resolution] inc_r ON inc.incidentid=inc_r._incidentid_value
# #     inner JOIN [Jubilee Business Line] bus on inc._jub_businessline_value=bus.jub_businesslineid
# #     INNER JOIN [Jubilee Team Members] tm on inc._ownerid_value = tm.systemuserid
# #     INNER JOIN [Jubilee Teams] teams on tm.teamid = teams.teamid
# #     INNER JOIN [Jubilee Users] users on inc._owninguser_value=users.systemuserid

# # where CAST(inc.createdon as datetime) >= '2024-04-01'
# # AND casetypecode in(5,6)
# # and (((SELECT COUNT(1) FROM [Jubilee Team Members] WHERE [Jubilee Team Members].systemuserid =inc._ownerid_value ) > 1 and teams.name != 'onejubileecrm')
# # or ((SELECT COUNT(1) FROM [Jubilee Team Members] WHERE [Jubilee Team Members].systemuserid =inc._ownerid_value ) = 1))

# # -- GO
# # -- SELECT COUNT(*) FROM [Jubilee Incidents ] WHERE casetypecode IN (5,6)


# # [31/01 11:04] Kelvin Otieno Okoth
# # jubileeinsurancewrkspc-dev.sql.azuresynapse.net

# # Credential's : crm_user / 20!2UX$aD1m572!0

# # DWHdevpool


# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO

# ALTER VIEW [dbo].[Test_Jubilee_Claims_Journey_TAT] AS
# WITH RankedAudits AS (
#     SELECT 
#         new_customername,
#         new_casenumber,
#         new_caseorigin,
#         new_usertat,
#         new_name,
#         new_textsystemuser,
#         new_textteam,
#         new_textowner,
#         CAST(new_previouslyassigned as datetime) as new_previouslyassigned,
#         CAST(createdon AS datetime) as createdon,
#         statecode,
#         new_caseduedate,
#         new_dateuploaded,
#         new_claimpaymentstatus,
#         new_voucherapprovaltat,
#         new_voucher,
#         new_voucherofauploadtat,
#         new_voucherverificationtat,
#         new_vouchercreationtat,
#         _new_case_value,
#         new_dateapproved,
#         new_dateresolved,
#         new_casestatus,
#         new_claimtype,
#         new_policynumber,
#         new_casetype,
#         new_dateprocessed,
#         new_dateassigned,
#         new_voucherverificationdate,
#         CAST(new_incidentcreatedon as datetime) AS [Case Creation Date],
#         new_userregion,
#         ROW_NUMBER() OVER (PARTITION BY new_casenumber ORDER BY (SELECT NULL)) as rn
#     FROM [Jubilee Audits]
# )

# SELECT 
#     r1.[Case Creation Date],
#     r1.new_userregion AS Country,
#     r1.new_textteam,
#     r1.new_casetype,
#     r1.new_policynumber,
#     r1.new_claimtype,
#     COALESCE(r1.new_voucher, r2.new_voucher, r3.new_voucher, r4.new_voucher ) as [TAT for Processor],
#     COALESCE(r1.new_voucherapprovaltat, r2.new_voucherapprovaltat, r3.new_voucherapprovaltat, r4.new_voucherapprovaltat ) as [TAT for Approver],
#     COALESCE(r1.new_voucherofauploadtat, r2.new_voucherofauploadtat, r3.new_voucherofauploadtat, r4.new_voucherofauploadtat ) AS [TAT OFA Upload],
#     COALESCE(r1.new_voucherverificationtat, r2.new_voucherverificationtat, r3.new_voucherverificationtat, r4.new_voucherverificationtat ) as [TAT for Verifier],
#     -- r1.new_voucher AS [TAT for processor],
#     COALESCE(r1.new_dateapproved, r2.new_dateapproved, r3.new_dateapproved, r4.new_dateapproved ) as [Voucher Approval Date], 
#     COALESCE(r1.new_dateuploaded, r2.new_dateuploaded, r3.new_dateuploaded, r4.new_dateuploaded ) as [Voucher OFA Upload Date],
#     COALESCE(r1.new_dateprocessed, r2.new_dateprocessed, r3.new_dateprocessed, r4.new_dateprocessed ) as [Voucher Process Date],
#     COALESCE(r1.new_voucherverificationdate, r2.new_voucherverificationdate, r3.new_voucherverificationdate, r4.new_voucherverificationdate,r5.new_voucherverificationdate )as [Voucher Verification Date],
#     COALESCE(r1.new_caseduedate, r2.new_caseduedate, r3.new_caseduedate,r4.new_caseduedate ) as [Maturity Date],
#     r1.new_casenumber,
#     r1.new_caseorigin,
#     r1.new_name,
#     r1.createdon,
#     r1.new_customername,
#     r1.statecode,
#     r1.new_casestatus,
#     r1.new_textsystemuser as First_Owner,
#     r1.new_usertat as FirstOwner_TAT,
#     r1.new_textowner as First_CaseOwner,
#     r2.new_textsystemuser as Second_Owner,
#     r2.new_textowner as Second_CaseOwner,
#     r2.new_usertat as second_user_tat,
#     r2.createdon as [Date of Assigning by CX to Claims],
    
#     --[Date of Re-assigning by Claims to CX],
#     r3.new_textsystemuser as third_owner,
#     r3.new_textowner as Third_CaseOwner,
#     r3.new_usertat as third_user_tat,
#     r3.createdon as [Date of Re-assigning by Claims to CX],
#     r4.new_textsystemuser as fourth_owner,
#     r4.new_textowner as Fourth_CaseOwner,
#     r4.new_usertat as fourth_user_tat,
#     r4.createdon as [Date of Re-assigning by CX to Claims],
#     r5.new_textsystemuser as fifth_name,
#     r5.new_textowner as Fifth_CaseOwner,
#     r5.new_usertat as fifth_user_tat,
#     jn.subject
# FROM RankedAudits r1
# LEFT JOIN RankedAudits r2 ON r1.new_casenumber = r2.new_casenumber AND r2.rn = 2
# LEFT JOIN RankedAudits r3 ON r1.new_casenumber = r3.new_casenumber AND r3.rn = 3
# LEFT JOIN RankedAudits r4 ON r1.new_casenumber = r4.new_casenumber AND r4.rn = 4
# LEFT JOIN RankedAudits r5 ON r1.new_casenumber = r5.new_casenumber AND r5.rn = 5
# LEFT JOIN [JubileeUAT Notes] jn ON r1._new_case_value = jn._objectid_value

# WHERE r1.rn = 1 AND (r1.new_casetype='Life Claim' OR r1.new_casetype='Pension Claim')


# GO
