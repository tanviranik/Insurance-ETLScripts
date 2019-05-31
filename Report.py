import pandas as pd 
import urllib

df = pd.read_csv("finalapi.csv") 
df["MONTHS"] = df.MONTHS.map("{:02}".format)
df['PERIOD'] = df["STAT_PROFILE_DATE_YEAR"].map(str) + '-' + df["MONTHS"].map(str) + '-01'
df['PERIOD'] = pd.to_datetime(df['PERIOD'])

report = df[(df['PERIOD'] >= '2005-01-01') & (df['PERIOD'] <= '2010-12-01')].groupby(['PROD_LINE', 'AGENCY_ID'])[['NB_WRTN_PREM_AMT', 'WRTN_PREM_AMT', 'PREV_WRTN_PREM_AMT', 'PRD_ERND_PREM_AMT']].sum()
print(report.head())
df.to_csv('Report.csv')

