import pandas as pd 
import sqlalchemy
import urllib

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=45.115.115.75,1460;DATABASE=Demo;UID=tesla;PWD=tesla1234")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#Dropping existing table
with engine.connect() as con:
    rs = con.execute(""" DROP TABLE [dbo].[InsuranceData]""")

#creating the table in sql server
with engine.connect() as con:
    rs = con.execute("""CREATE TABLE [dbo].[InsuranceData](
	                        [AGENCY_ID] [bigint] NULL,
	                        [PRIMARY_AGENCY_ID] [bigint] NULL,
	                        [PROD_ABBR] [varchar](max) NULL,
	                        [PROD_LINE] [varchar](max) NULL,
	                        [STATE_ABBR] [varchar](max) NULL,
	                        [STAT_PROFILE_DATE_YEAR] [bigint] NULL,
	                        [RETENTION_POLY_QTY] [bigint] NULL,
	                        [POLY_INFORCE_QTY] [bigint] NULL,
	                        [PREV_POLY_INFORCE_QTY] [bigint] NULL,
	                        [NB_WRTN_PREM_AMT] [float] NULL,
	                        [WRTN_PREM_AMT] [float] NULL,
	                        [PREV_WRTN_PREM_AMT] [float] NULL,
	                        [PRD_ERND_PREM_AMT] [float] NULL,
	                        [PRD_INCRD_LOSSES_AMT] [float] NULL,
	                        [MONTHS] [bigint] NULL,
	                        [RETENTION_RATIO] [float] NULL,
	                        [LOSS_RATIO] [float] NULL,
	                        [LOSS_RATIO_3YR] [float] NULL,
	                        [GROWTH_RATE_3YR] [float] NULL,
	                        [AGENCY_APPOINTMENT_YEAR] [bigint] NULL,
	                        [ACTIVE_PRODUCERS] [bigint] NULL,
	                        [MAX_AGE] [bigint] NULL,
	                        [MIN_AGE] [bigint] NULL,
	                        [VENDOR_IND] [varchar](max) NULL,
	                        [VENDOR] [varchar](max) NULL,
	                        [PL_START_YEAR] [bigint] NULL,
	                        [PL_END_YEAR] [bigint] NULL,
	                        [COMMISIONS_START_YEAR] [bigint] NULL,
	                        [COMMISIONS_END_YEAR] [bigint] NULL,
	                        [CL_START_YEAR] [bigint] NULL,
	                        [CL_END_YEAR] [bigint] NULL,
	                        [ACTIVITY_NOTES_START_YEAR] [bigint] NULL,
	                        [ACTIVITY_NOTES_END_YEAR] [bigint] NULL,
	                        [CL_BOUND_CT_MDS] [bigint] NULL,
	                        [CL_QUO_CT_MDS] [bigint] NULL,
	                        [CL_BOUND_CT_SBZ] [bigint] NULL,
	                        [CL_QUO_CT_SBZ] [bigint] NULL,
	                        [CL_BOUND_CT_eQT] [bigint] NULL,
	                        [CL_QUO_CT_eQT] [bigint] NULL,
	                        [PL_BOUND_CT_ELINKS] [bigint] NULL,
	                        [PL_QUO_CT_ELINKS] [bigint] NULL,
	                        [PL_BOUND_CT_PLRANK] [bigint] NULL,
	                        [PL_QUO_CT_PLRANK] [bigint] NULL,
	                        [PL_BOUND_CT_eQTte] [bigint] NULL,
	                        [PL_QUO_CT_eQTte] [bigint] NULL,
	                        [PL_BOUND_CT_APPLIED] [bigint] NULL,
	                        [PL_QUO_CT_APPLIED] [bigint] NULL,
	                        [PL_BOUND_CT_TRANSACTNOW] [bigint] NULL,
	                        [PL_QUO_CT_TRANSACTNOW] [bigint] NULL
                        )""")


data = pd.read_csv("finalapi.csv") 

#Checking data quality
print('Dataframe: ')
print(data.head())

print('Checking null items: ')
print(data.isnull())
print(data.isnull().values.any())

print('Finding the column with null items: ')
print(data.columns[data.isnull().any()])

print('Describing the data to get statistical information: ')
print(data.describe())

data.to_sql('InsuranceData', con=engine, if_exists='replace', index=False)
