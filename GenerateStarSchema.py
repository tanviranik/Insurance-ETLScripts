import pandas as pd 
import sqlalchemy
import urllib

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=45.115.115.75,1460;DATABASE=Demo;UID=tesla;PWD=tesla1234")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#Dropping existing table
with engine.connect() as con:
    rs = con.execute(""" drop table DimAgencyDetails """)

#creating the table in sql server
with engine.connect() as con:
    rs = con.execute("""SELECT ROW_NUMBER() OVER ( ORDER BY [AGENCY_ID] ) AS AGENCY_DETAILS_ID, AgencyDetails.*
                        INTO DimAgencyDetails
                        FROM (
	                        SELECT distinct 
		                        [AGENCY_ID], 
		                        [PRIMARY_AGENCY_ID], 
		                        [AGENCY_APPOINTMENT_YEAR]
		                        ,[ACTIVE_PRODUCERS]
		                        ,[MAX_AGE]
		                        ,[MIN_AGE]
		                        ,[PL_START_YEAR]
		                        ,[PL_END_YEAR]
		                        ,[COMMISIONS_START_YEAR]
		                        ,[COMMISIONS_END_YEAR]
		                        ,[CL_START_YEAR]
		                        ,[CL_END_YEAR]
		                        ,[ACTIVITY_NOTES_START_YEAR]
		                        ,[ACTIVITY_NOTES_END_YEAR]
	                        FROM [dbo].[InsuranceData] 
                        ) AS AgencyDetails""")

with engine.connect() as con:
    rs = con.execute(""" drop table DimPolicyDetails """)

with engine.connect() as con:
    rs = con.execute("""SELECT ROW_NUMBER() OVER ( ORDER BY [CL_BOUND_CT_MDS]) AS POLICY_DETAILS_ID, PolicyDetails.*
                            INTO DimPolicyDetails
                            FROM
                            (
	                            SELECT DISTINCT  [CL_BOUND_CT_MDS]
                                  ,[CL_QUO_CT_MDS]
                                  ,[CL_BOUND_CT_SBZ]
                                  ,[CL_QUO_CT_SBZ]
                                  ,[CL_BOUND_CT_eQT]
                                  ,[CL_QUO_CT_eQT]
                                  ,[PL_BOUND_CT_ELINKS]
                                  ,[PL_QUO_CT_ELINKS]
                                  ,[PL_BOUND_CT_PLRANK]
                                  ,[PL_QUO_CT_PLRANK]
                                  ,[PL_BOUND_CT_eQTte]
                                  ,[PL_QUO_CT_eQTte]
                                  ,[PL_BOUND_CT_APPLIED]
                                  ,[PL_QUO_CT_APPLIED]
	                              ,[PL_BOUND_CT_TRANSACTNOW]
                                  ,[PL_QUO_CT_TRANSACTNOW] 
	                            FROM [dbo].[InsuranceData]
                            ) as PolicyDetails""")


with engine.connect() as con:
    rs = con.execute(""" drop table DimPeriod """)

with engine.connect() as con:
    rs = con.execute("""SELECT ROW_NUMBER() OVER ( ORDER BY [STAT_PROFILE_DATE_YEAR], [MONTHS] ) AS PERIOD_ID, PeriodDetails.*
                        INTO DimPeriod
                        FROM
                        (
	                        SELECT DISTINCT 
		                        [STAT_PROFILE_DATE_YEAR]
		                        ,[MONTHS]
	                        FROM [dbo].[InsuranceData]
                        ) as PeriodDetails""")

with engine.connect() as con:
    rs = con.execute(""" drop table DimProd """)

with engine.connect() as con:
    rs = con.execute("""SELECT ROW_NUMBER() OVER ( ORDER BY [PROD_ABBR]
		                        ,[PROD_LINE]
		                        ,[STATE_ABBR]) AS PROD_DETAILS_ID, ProdDetails.*
                        INTO DimProd
                        FROM
                        (
	                        SELECT DISTINCT 
		                        [PROD_ABBR]
		                        ,[PROD_LINE]
		                        ,[STATE_ABBR]
	                        FROM [dbo].[InsuranceData]
                        ) as ProdDetails""")

with engine.connect() as con:
    rs = con.execute(""" drop table DimVendor """)

with engine.connect() as con:
    rs = con.execute("""SELECT ROW_NUMBER() OVER ( ORDER BY [VENDOR_IND], [VENDOR] ) AS VENDOR_ID, VendorDetails.*
                        INTO DimVendor
                        FROM
                        (
	                        SELECT DISTINCT 
		                        [VENDOR_IND]
		                        ,[VENDOR]
	                        FROM [dbo].[InsuranceData]
                        ) as VendorDetails""")


### Preparing the FACT table
with engine.connect() as con:
    rs = con.execute(""" drop table FactInsurance """)

with engine.connect() as con:
    rs = con.execute("""SELECT 
		                    CAST([AGENCY_DETAILS_ID] as varchar(20)) + CAST(POLICY_DETAILS_ID as varchar(20)) +  CAST(PERIOD_ID as varchar(20)) +  CAST(PROD_DETAILS_ID as varchar(20)) +  CAST(VENDOR_ID as varchar(20)) as COMPOSITE_ID
		                    ,[AGENCY_DETAILS_ID]
		                    --[AGENCY_ID]
                      --    ,[PRIMARY_AGENCY_ID]
	                     -- ,[AGENCY_APPOINTMENT_YEAR]
                      --    ,[ACTIVE_PRODUCERS]
                      --    ,[MAX_AGE]
                      --    ,[MIN_AGE]
	                     -- ,[PL_START_YEAR]
                      --    ,[PL_END_YEAR]
                      --    ,[COMMISIONS_START_YEAR]
                      --    ,[COMMISIONS_END_YEAR]
                      --    ,[CL_START_YEAR]
                      --    ,[CL_END_YEAR]
                      --    ,[ACTIVITY_NOTES_START_YEAR]
                      --    ,[ACTIVITY_NOTES_END_YEAR]

		                    ,POLICY_DETAILS_ID
	                      --,[CL_BOUND_CT_MDS]
                       --   ,[CL_QUO_CT_MDS]
                       --   ,[CL_BOUND_CT_SBZ]
                       --   ,[CL_QUO_CT_SBZ]
                       --   ,[CL_BOUND_CT_eQT]
                       --   ,[CL_QUO_CT_eQT]
                       --   ,[PL_BOUND_CT_ELINKS]
                       --   ,[PL_QUO_CT_ELINKS]
                       --   ,[PL_BOUND_CT_PLRANK]
                       --   ,[PL_QUO_CT_PLRANK]
                       --   ,[PL_BOUND_CT_eQTte]
                       --   ,[PL_QUO_CT_eQTte]
                       --   ,[PL_BOUND_CT_APPLIED]
                       --   ,[PL_QUO_CT_APPLIED]
                       --   ,[PL_BOUND_CT_TRANSACTNOW]
                       --   ,[PL_QUO_CT_TRANSACTNOW]

		                    ,PERIOD_ID
	                      --,[STAT_PROFILE_DATE_YEAR]
	                      --,[MONTHS]

		                    ,PROD_DETAILS_ID
                          --,[PROD_ABBR]
                          --,[PROD_LINE]
                          --,[STATE_ABBR]

	                      ,VENDOR_ID
      
                          ,[RETENTION_POLY_QTY]
                          ,[POLY_INFORCE_QTY]
                          ,[PREV_POLY_INFORCE_QTY]
                          ,[NB_WRTN_PREM_AMT]
                          ,[WRTN_PREM_AMT]
                          ,[PREV_WRTN_PREM_AMT]
                          ,[PRD_ERND_PREM_AMT]
                          ,[PRD_INCRD_LOSSES_AMT]
                          ,[RETENTION_RATIO]
                          ,[LOSS_RATIO]
                          ,[LOSS_RATIO_3YR]
                          ,[GROWTH_RATE_3YR]
                    INTO FactInsurance
                    FROM [dbo].[InsuranceData] INS INNER JOIN dbo.DimAgencyDetails DAG
                    ON INS.AGENCY_ID = DAG.AGENCY_ID
	                    AND INS.[AGENCY_ID] = DAG.[AGENCY_ID]
	                    AND INS.[PRIMARY_AGENCY_ID] = DAG.[PRIMARY_AGENCY_ID]
	                    AND INS.[AGENCY_APPOINTMENT_YEAR] = DAG.[AGENCY_APPOINTMENT_YEAR]
	                    AND INS.[ACTIVE_PRODUCERS] = DAG.[ACTIVE_PRODUCERS]
	                    AND INS.[MAX_AGE] = DAG.[MAX_AGE]
	                    AND INS.[MIN_AGE] = DAG.[MIN_AGE]
	                    AND INS.[PL_START_YEAR] = DAG.[PL_START_YEAR]
	                    AND INS.[PL_END_YEAR] = DAG.[PL_END_YEAR]
	                    AND INS.[COMMISIONS_START_YEAR] = DAG.[COMMISIONS_START_YEAR]
	                    AND INS.[COMMISIONS_END_YEAR] = DAG.[COMMISIONS_END_YEAR]
	                    AND INS.[CL_START_YEAR] = DAG.[CL_START_YEAR]
	                    AND INS.[CL_END_YEAR] = DAG.[CL_END_YEAR]
	                    AND INS.[ACTIVITY_NOTES_START_YEAR] = DAG.[ACTIVITY_NOTES_START_YEAR]
	                    AND INS.[ACTIVITY_NOTES_END_YEAR] = DAG.[ACTIVITY_NOTES_END_YEAR]

                    INNER JOIN dbo.DimPolicyDetails DPL ON 
	                    INS.[CL_BOUND_CT_MDS] = DPL.[CL_BOUND_CT_MDS]
                        AND  INS.[CL_QUO_CT_MDS] = DPL.[CL_QUO_CT_MDS]
                        AND  INS.[CL_BOUND_CT_SBZ] = DPL.[CL_BOUND_CT_SBZ]
                        AND   INS.[CL_QUO_CT_SBZ] = DPL.[CL_QUO_CT_SBZ]
                        AND   INS.[CL_BOUND_CT_eQT] = DPL.[CL_BOUND_CT_eQT]
                        AND   INS.[CL_QUO_CT_eQT] = DPL.[CL_QUO_CT_eQT]
                        AND   INS.[PL_BOUND_CT_ELINKS] = DPL.[PL_BOUND_CT_ELINKS]
                        AND   INS.[PL_QUO_CT_ELINKS] = DPL.[PL_QUO_CT_ELINKS]
                        AND   INS.[PL_BOUND_CT_PLRANK] = DPL.[PL_BOUND_CT_PLRANK]
                        AND   INS.[PL_QUO_CT_PLRANK] = DPL.[PL_QUO_CT_PLRANK]
                        AND   INS.[PL_BOUND_CT_eQTte] = DPL.[PL_BOUND_CT_eQTte]
                        AND   INS.[PL_QUO_CT_eQTte] = DPL.[PL_QUO_CT_eQTte]
                        AND   INS.[PL_BOUND_CT_APPLIED] = DPL.[PL_BOUND_CT_APPLIED]
                        AND   INS.[PL_QUO_CT_APPLIED] = DPL.[PL_QUO_CT_APPLIED]
	                    AND   INS.[PL_BOUND_CT_TRANSACTNOW] = DPL.[PL_BOUND_CT_TRANSACTNOW]
                        AND   INS.[PL_QUO_CT_TRANSACTNOW] = DPL.[PL_QUO_CT_TRANSACTNOW]

                    INNER JOIN dbo.DimPeriod DPR ON 
	                    INS.[STAT_PROFILE_DATE_YEAR] = DPR.[STAT_PROFILE_DATE_YEAR]
	                    AND INS.[MONTHS] = DPR.[MONTHS]

                    INNER JOIN dbo.DimProd DPROD ON 
	                    INS.[PROD_ABBR] = DPROD.[PROD_ABBR]
                        AND INS.[PROD_LINE] = DPROD.[PROD_LINE]
                        AND INS.[STATE_ABBR] = DPROD.[STATE_ABBR]

                    INNER JOIN dbo.DimVendor DVND ON
	                    INS.[VENDOR_IND] = DVND.[VENDOR_IND]
                        AND INS.[VENDOR] = DVND.[VENDOR]""")