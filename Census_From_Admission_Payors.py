# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 14:16:17 2019

@author: kpoon
"""


import sys
import pandas as pd
sys.path.append("C:/Users/kpoon/Desktop/KenPoon/Self/Python")
from Functions import * 
import numpy as np
import datetime as dt

ATD = "C:/Users/kpoon/Desktop/KenPoon/Self/Python/MySQL/ATD_PowerBI_withoutPayor.sql"
###"Hi, Stella Baby"


###Get Data From SQL
ATDquerys=[]
ATDquerys= getQueryStatement(ATD)
exeQuery = ATDquerys[:len(ATDquerys)-2]
df = runQuery(exeQuery, ATDquerys[len(ATDquerys)-1])

####Took out same day discharge, should be eie 
SameDayDischarge = df["datedischarge"].isnull()*df["status"]=="Discharged"
SameDayDischarge1 = (df["datedischarge"] == "0000-00-00") & (df["status"]=="Discharged")

dfClean = df[(SameDayDischarge == False)&(SameDayDischarge1 == False)]


##list of index for different status
admitStatus = dfClean["status"]=="Admitted"
transferStatus = dfClean["status"]=="Transfer"
dischargeStatus = dfClean["status"]=="Discharged" 



### set endDate for different status
dfClean["EndDate"]=pd.to_datetime('today')
dfClean["EndDate"][admitStatus]= pd.to_datetime('today')
dfClean["EndDate"][dischargeStatus]= pd.to_datetime(dfClean["datedischarge"][dischargeStatus])
dfClean["EndDate"][transferStatus]= pd.to_datetime(dfClean["datetransfer"][transferStatus])
dfClean["StartDate"] = pd.to_datetime(dfClean["dateadmission"])

dfClean["EndDate"]=dfClean["EndDate"].dt.date
dfClean["datedischarge"] = dfClean["datedischarge"].fillna(pd.to_datetime("2018-9-30").date())
dfClean["TotalEP"] = dfClean.groupby(["client_id"])["episode_id"].nunique()+1
dfClean["TotalEP"] = dfClean["TotalEP"].fillna(1)


td = pd.Timedelta(1, unit = 'd')

dfClean['_dt_diff'] = dfClean["EndDate"] - dfClean["StartDate"].dt.date
#### get the maximum timediff:
max_diff = int((dfClean['_dt_diff'] / td).max())

###create a separate table for each days with dates
df_diffs = pd.concat([pd.DataFrame({'_to_add': np.arange(0, dt_diff + 1) * td}).assign(_dt_diff=dt_diff * td)
                          for dt_diff in range(max_diff + 1)])


##join to the original dataframe
data_expanded = dfClean.merge(df_diffs,how = 'left', left_on='_dt_diff', right_on='_dt_diff')

#### the new dt column is just start plus the intermediate diffs:
data_expanded["serviceDate"] = data_expanded["StartDate"] + data_expanded['_to_add']
data_expanded["serviceDate"]= data_expanded["serviceDate"].dt.date
data_expanded["StartDate"]=data_expanded["StartDate"].dt.date



data_expanded['CensusCount'] = data_expanded.groupby(['client_id','serviceDate'])['admission_id'].transform('count')
data_expanded['MaxCreated'] = data_expanded.groupby(['client_id','serviceDate'])['created_at'].transform('max')
data_expanded['Ep_Admit'] = data_expanded.groupby(['episode_id'])['dateadmission'].transform('min')
data_expanded['Treatment_Admit'] = data_expanded.groupby(['episode_id','treatmentNumber'])['dateadmission'].transform('min')
data_expanded['Treatment_Discharge'] = data_expanded.groupby(['episode_id','treatmentNumber'])['datedischarge'].transform('max')
data_expanded['Treatment_Dctype'] = data_expanded.groupby(['client_id','treatmentNumber'])['dctype'].transform('last')
data_expanded['Episode_Discharge'] = data_expanded.groupby(['episode_id'])['datedischarge'].transform('max')


moreThanOneCensus = (data_expanded['CensusCount']>1)
oneCensus = (data_expanded['CensusCount']==1)
MaxCreated = (data_expanded['MaxCreated']==data_expanded['created_at'])
data_expanded = data_expanded[  oneCensus | (moreThanOneCensus & MaxCreated)]

    # don't modify dataframe in place:
del dfClean
del df
del df_diffs


######################################################
payor = "C:/Users/kpoon/Desktop/KenPoon/Self/Python/MySQL/PayorTransaction.sql"

payorquerys=[]
payorquerys= getQueryStatement(payor)
exeQuery = payorquerys[:len(payorquerys)-1]
statement = payorquerys[len(payorquerys)-1]
payorDf = runQuery(exeQuery, statement)
clean = get_CleanLists()

payorClean = pd.merge(payorDf, clean["Insurance"], how = 'left', left_on = 'payorname', right_on = 'input')
payorClean= payorClean.drop(columns=['CID_temp', 'END_temp'])
payorClean["payorStartDate"] = pd.to_datetime(payorClean["payorStartDate"],errors='coerce').dt.date
payorClean["payorEndDate"] = pd.to_datetime(payorClean["payorEndDate"],errors='coerce').dt.date
payorClean['Count'] = payorClean.groupby('client_id')['client_id'].transform('count')
payorClean['MaxStartDate'] = payorClean.groupby('client_id')['payorStartDate'].transform('min')
payorClean['sameStartDate'] = payorClean.groupby(['client_id','payorStartDate'])['client_id'].transform('count')
payorClean['minDatedoc'] = payorClean.groupby('client_id')['datedoc'].transform('min')
payorClean["Ep_payor_count"] = payorClean.groupby('episode_id')['id'].transform('count')
payorClean["Ep_payor"] = payorClean.groupby('episode_id')['PayorBucket'].transform('last')
payorClean["Ep_payor_CleanInsurance"] = payorClean.groupby('episode_id')['CleanedInsurance'].transform('last')
payorClean["Ep_payor"] =np.where(payorClean["Ep_payor_count"].isnull(),payorClean["PayorBucket"],payorClean["Ep_payor"] )
payorClean["Ep_firstInsurance"]=np.where( (payorClean["Ep_payor"] == payorClean["PayorBucket"]) | \
          (payorClean["Ep_payor_count"] ==1) | (payorClean["episode_id"].isnull())
          , 1, 0)
payorClean["Ep_PayorType"] = np.where(payorClean["fundingsrc"] == "Insurance" , 1, 0 )
payorClean["Ep_PayorType"] = payorClean.groupby('episode_id')['Ep_PayorType'].transform('max')
payorClean["Ep_PayorType"] = np.where(payorClean["Ep_PayorType"] == 1 & pd.notnull(payorClean["episode_id"]),\
          "Insurance", payorClean["fundingsrc"] )
payorClean= payorClean.drop(columns=['episode_id'])


for i in payorClean.index:
    val=  payorClean.get_value(i,"sameStartDate")
    if val >1  and payorClean.get_value(i,"datedoc") != payorClean.get_value(i,"minDatedoc"):
        payorClean.set_value(i, "payorStartDate",payorClean.get_value(i,"datedoc")) 



payorClean= payorClean.drop(columns=['minDatedoc', 'datedoc', 'sameStartDate'])


df = pd.merge(data_expanded, payorClean, how='left', left_on='client_id', right_on='client_id')
df["inRange"] = ((df['serviceDate']>=df['payorStartDate']) & (df['serviceDate']<df['payorEndDate']))
df["atLeastOneInrange"] = df.groupby(['client_id','serviceDate'])['inRange'].transform('sum')

latest = (df["MaxStartDate"]== df["payorStartDate"])
noneInrange = (df["atLeastOneInrange"]==0)
noPayor = (df['payorStartDate'].isnull())    
only1Payor = (df['Count']==1)  
inRange = ((df['serviceDate']>=df['payorStartDate']) & (df['serviceDate']<df['payorEndDate']))
moreThan1payor = (df['Count']>1)


allPayorsNotValid_UseOldestInstead = (moreThan1payor & noneInrange & latest )

df['created_at'] = pd.to_datetime(df['created_at'] )





df =       df[  inRange |  noPayor | only1Payor  | allPayorsNotValid_UseOldestInstead]


del data_expanded
del payorClean
del payorDf


#
savedFileAt = "S:/Accounting/Month End/Revenue/Anaheim Revenue/2019/Revenue/Published/Sources"
df.to_csv('{0}\{1}'.format(savedFileAt,"CensusAdmission.csv"), header = True, index= False)


