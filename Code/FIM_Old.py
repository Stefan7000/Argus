# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 21:07:21 2019

@author: STELAR ARGUS
"""
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
import pyodbc
import sys
sys.path.insert(0, 'C:\\Code\\')
import steff_funclib
import steff_funbblib
import steff_funpltlib

sql_con = pyodbc.connect('Driver={SQL Server};'
                      'Server=FRACTAL\SQLEXPRESS;'
                      'Database=stef_db;'
                      'Trusted_Connection=yes;')


#sql_con.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_SERIALIZABLE)



def GetHSYTM_YC(YC_Name,calcdate):
    sqlQuery='SELECT fldYCParameter,fldValue FROM tblFIMYieldCurveData WHERE fldName=\'' + YC_Name + '\' AND fldDate=CONVERT(DATETIME,\'' + calcdate + '\',120)'
    df_YC_T_Yield=pd.read_sql(sqlQuery, sql_con)
    df_YC_T_Yield['fldYCParameter'].replace('Y', '', regex=True,inplace=True)
    df_YC_T_Yield['fldYCParameter']=df_YC_T_Yield['fldYCParameter'].astype('float')
    df_YC_T_Yield.sort_values('fldYCParameter', inplace=True, ascending=True)
    return df_YC_T_Yield


def LoadSwapQuotes(startdate,enddate):
    ''' Load all swap rates to Yield curves. Rem: only MID'''
    cursor=sql_con.cursor()
    swaprate_SQL="SELECT fldBBCode FROM tblFIMSwap ORDER BY fldCurrency,fldT2M;"
    df_swaprates_ticker=pd.read_sql(swaprate_SQL, sql_con)
    field_list=['MID'] #No options
    TickerList=df_swaprates_ticker['fldBBCode'].tolist()
    df_swaprates=steff_funbblib.bbhist(TickerList,field_list,startdate,enddate)
    #Remove rows with pd.np.nan
    df_swaprates.dropna(inplace = True)
    
    #********** Delete before insert
    # Remove permanently ' Corp' after isin
    TickerList=df_swaprates['Ticker'].unique()
    TickerList = '\',\''.join(map(str, TickerList))
    TickerList='(\'' + TickerList + '\')'
    strSQLDelete='DELETE FROM tblFIMSwapPrices WHERE (fldDate BETWEEN CONVERT(DATETIME,\'' + startdate + '\',120) AND CONVERT(DATETIME,\'' + enddate + '\',120)) AND fldBBCode IN ' + TickerList + ';'
    cursor.execute(strSQLDelete)
    sql_con.commit()    
    #********** Insert prices
    InsertData = list(zip(df_swaprates['Ticker'].astype(str),df_swaprates['Date'].astype(str),df_swaprates['MID'].astype(float)))
    strSQL=";INSERT INTO tblFIMSwapPrices VALUES ".join(str(x) for x in InsertData)
    strSQL="SET NOCOUNT ON INSERT INTO tblFIMSwapPrices VALUES " + strSQL    
    cursor.execute(strSQL)    
    sql_con.commit()
    #sql_con.close()
    
    '''
    #********** Insert prices
    InsertData = list(zip(df_swaprates['Ticker'].astype(str),df_swaprates['Date'].astype(str),df_swaprates['MID'].astype(str)))
    cursor.executemany("INSERT INTO tblFIMSwapPrices (fldBBCode,fldDate,fldValue) VALUES(?, ?, ?)",InsertData)
    sql_con.commit()
    sql_con.close()
    '''
    
def LoadShortRatesQuotes(startdate,enddate):
    ''' Load all short rates to Yield curves (Yield when T=0 not swap curves). Rem: only PX_LAST'''
    cursor=sql_con.cursor()
    shortrate_SQL="SELECT fldShortRate,fldShortRateBBField FROM tblFIMYCTemplateBond;"
    df_shortrates_tickerField=pd.read_sql(shortrate_SQL, sql_con)
    field_list=['PX_LAST'] #No options
    TickerList=df_shortrates_tickerField['fldShortRate'].tolist()
    df_shortrates=steff_funbblib.bbhist(TickerList,field_list,startdate,enddate)
    #Remove rows with pd.np.nan
    df_shortrates.dropna(inplace = True)
    
    #********** Delete before insert
    # Remove permanently ' Corp' after isin
    TickerList=df_shortrates['Ticker'].unique()
    TickerList = '\',\''.join(map(str, TickerList))
    TickerList='(\'' + TickerList + '\')'
    strSQLDelete='DELETE FROM tblFIMShortRatePrices WHERE (fldDate BETWEEN CONVERT(DATETIME,\'' + startdate + '\',120) AND CONVERT(DATETIME,\'' + enddate + '\',120)) AND fldShortRate IN ' + TickerList + ';'
    cursor.execute(strSQLDelete)
    sql_con.commit()
    
    #********** Insert prices
    df_shortrates['fldShortRateBBField']='PX_LAST' #Force a new column
    InsertData = list(zip(df_shortrates['Ticker'].astype(str),df_shortrates['fldShortRateBBField'].astype(str),df_shortrates['Date'].astype(str),df_shortrates['PX_LAST'].astype(float)))
    strSQL=";INSERT INTO tblFIMShortRatePrices VALUES ".join(str(x) for x in InsertData)
    strSQL="SET NOCOUNT ON INSERT INTO tblFIMShortRatePrices VALUES " + strSQL    
    cursor.execute(strSQL)    
    sql_con.commit()
    #sql_con.close()
    '''  
    #********** Insert prices
    df_shortrates['fldShortRateBBField']='PX_LAST' #Force a new column
    InsertData = list(zip(df_shortrates['Ticker'].astype(str),df_shortrates['fldShortRateBBField'].astype(str),df_shortrates['Date'].astype(str),df_shortrates['PX_LAST'].astype(str)))
    cursor.executemany("INSERT INTO tblFIMShortRatePrices (fldShortRate,fldShortRateBBField,fldDate,fldValue) VALUES(?, ?, ?, ?)",InsertData)
    sql_con.commit()
    sql_con.close()
    '''
    
def LoadShortRatesQuotes_swapcurve(startdate,enddate):
    ''' Load all short rates to Yield curves (Yield when T=0 not swap curves). Rem: only PX_LAST'''
    cursor=sql_con.cursor()
    shortrate_SQL="SELECT fldShortRate,fldShortRateBBField FROM tblFIMYCTemplateSwap;"
    df_shortrates_tickerField=pd.read_sql(shortrate_SQL, sql_con)
    field_list=['PX_LAST'] #No options
    TickerList=df_shortrates_tickerField['fldShortRate'].tolist()
    df_shortrates=steff_funbblib.bbhist(TickerList,field_list,startdate,enddate)
    #Remove rows with pd.np.nan
    df_shortrates.dropna(inplace = True)
    
    #********** Delete before insert
    # Remove permanently ' Corp' after isin
    TickerList=df_shortrates['Ticker'].unique()
    TickerList = '\',\''.join(map(str, TickerList))
    TickerList='(\'' + TickerList + '\')'
    strSQLDelete='DELETE FROM tblFIMShortRatePrices WHERE (fldDate BETWEEN CONVERT(DATETIME,\'' + startdate + '\',120) AND CONVERT(DATETIME,\'' + enddate + '\',120)) AND fldShortRate IN ' + TickerList + ';'
    cursor.execute(strSQLDelete)
    sql_con.commit()
    
    #********** Insert prices
    df_shortrates['fldShortRateBBField']='PX_LAST' #Force a new column
    InsertData = list(zip(df_shortrates['Ticker'].astype(str),df_shortrates['fldShortRateBBField'].astype(str),df_shortrates['Date'].astype(str),df_shortrates['PX_LAST'].astype(float)))
    strSQL=";INSERT INTO tblFIMShortRatePrices VALUES ".join(str(x) for x in InsertData)
    strSQL="SET NOCOUNT ON INSERT INTO tblFIMShortRatePrices VALUES " + strSQL    
    cursor.execute(strSQL)    
    sql_con.commit()
    #sql_con.close()
    '''
    #********** Insert prices
    df_shortrates['fldShortRateBBField']='PX_LAST' #Force a new column
    InsertData = list(zip(df_shortrates['Ticker'].astype(str),df_shortrates['fldShortRateBBField'].astype(str),df_shortrates['Date'].astype(str),df_shortrates['PX_LAST'].astype(str)))
    cursor.executemany("INSERT INTO tblFIMShortRatePrices (fldShortRate,fldShortRateBBField,fldDate,fldValue) VALUES(?, ?, ?, ?)",InsertData)
    sql_con.commit()
    sql_con.close()
    '''
    
def LoadBondQuotes_fix_from_debt_ticker(debtticker_list,startdate,enddate):
    '''Load YLD_YTM_BID, PX_BID, PX_DIRTY_BID from Bloomberg given debt ticker in table tblFIMBond '''
    cursor=sql_con.cursor()
    IssuerList = '\',\''.join(map(str, debtticker_list))
    IssuerList='(\'' + IssuerList + '\')'
    strSQL = 'SELECT CONCAT(fldISIN,\' Corp\') AS ISIN,fldMaturity AS Mat FROM tblFIMBond WHERE fldMaturity>CONVERT(DATETIME,\'' + startdate + '\',120) AND fldIssueDate<=CONVERT(DATETIME,\'' + enddate + '\',120) AND fldTicker IN ' + IssuerList + ';'
    df_bonds=pd.read_sql(strSQL, sql_con)
    #Load prices from Bloomberg
    field_list=['YLD_YTM_BID', 'PX_BID', 'PX_DIRTY_BID']
    ISINList=df_bonds['ISIN'].tolist()
    df_Bondprice=steff_funbblib.bbhist(ISINList,field_list,startdate,enddate)
    #Remove rows with pd.np.nan
    df_Bondprice.dropna(inplace = True)
    #print(df_Bondprice)
    
    #********** Delete before insert
    # Remove permanently ' Corp' after isin
    df_Bondprice['Ticker'].replace(' Corp', '', regex=True,inplace=True)
    isinList=df_Bondprice['Ticker'].unique()
    isinList = '\',\''.join(map(str, isinList))
    isinList='(\'' + isinList + '\')'
    strSQLDelete='DELETE FROM tblFIMBondPrices WHERE (fldDate BETWEEN CONVERT(DATETIME,\'' + startdate + '\',120) AND CONVERT(DATETIME,\'' + enddate + '\',120)) AND fldISIN IN ' + isinList + ';'
    cursor.execute(strSQLDelete)
    sql_con.commit()
    #********** Insert prices
    InsertData = list(zip(df_Bondprice['Ticker'].astype(str), df_Bondprice['Date'].astype(str),df_Bondprice['YLD_YTM_BID'].astype(float),df_Bondprice['PX_BID'].astype(float),df_Bondprice['PX_DIRTY_BID'].astype(float)))
    strSQL=";INSERT INTO tblFIMBondPrices VALUES ".join(str(x) for x in InsertData)
    strSQL="SET NOCOUNT ON INSERT INTO tblFIMBondPrices VALUES " + strSQL    
    cursor.execute(strSQL)    
    sql_con.commit()
    #sql_con.close()
     
    '''
    InsertData = list(zip(df_Bondprice['Ticker'].astype(str), df_Bondprice['Date'].astype(str),df_Bondprice['YLD_YTM_BID'].astype(str),df_Bondprice['PX_BID'].astype(str),df_Bondprice['PX_DIRTY_BID'].astype(str)))
    cursor.executemany("INSERT INTO tblFIMBondPrices (fldISIN,fldDate,fldYTMBid,fldPXCleanBid,fldPXDirtyBid) VALUES(?, ?, ?, ?, ?)",InsertData)
    sql_con.commit()
    sql_con.close()
    '''

def LoadBondQuotes_fix_from_isin(isin_list,startdate,enddate):
    '''Load YLD_YTM_BID, PX_BID, PX_DIRTY_BID from Bloomberg given isin in table tblFIMBond '''
    isin_list = '\',\''.join(map(str, isin_list))
    isin_list='(\'' + isin_list + '\')'
    strSQL = 'SELECT CONCAT(fldISIN,\' Corp\') AS ISIN,fldMaturity AS Mat FROM tblFIMBond WHERE fldMaturity>CONVERT(DATETIME,\'' + startdate + '\',120) AND fldIssueDate<=CONVERT(DATETIME,\'' + enddate + '\',120) AND fldISIN IN ' + isin_list + ';'
    df_bonds=pd.read_sql(strSQL, sql_con)
    #Load prices from Bloomberg
    field_list=['YLD_YTM_BID', 'PX_BID', 'PX_DIRTY_BID']
    ISINList=df_bonds['ISIN'].tolist()
    df_Bondprice=steff_funbblib.bbhist(ISINList,field_list,startdate,enddate)
    
    cursor=sql_con.cursor()
    #********** Delete before insert
    # Remove permanently ' Corp' after isin
    df_Bondprice['Ticker'].replace(' Corp', '', regex=True,inplace=True)
    isinList=df_Bondprice['Ticker'].unique()
    isinList = '\',\''.join(map(str, isinList))
    isinList='(\'' + isinList + '\')'
    strSQLDelete='DELETE FROM tblFIMBondPrices WHERE (fldDate BETWEEN CONVERT(DATETIME,\'' + startdate + '\',120) AND CONVERT(DATETIME,\'' + enddate + '\',120)) AND fldISIN IN ' + isinList + ';'    
    cursor.execute(strSQLDelete)
    sql_con.commit()
    
    #********** Insert prices
    strSQLInsert=''
    strSQLInsert = [strSQLInsert+'INSERT INTO tblFIMBondPrices VALUES (\'' + fldISIN + '\',\'' + fldDate + '\',\'' + fldYTMBid + '\',\'' + fldPXCleanBid + '\',\'' + fldPXDirtyBid + '\');' 
                    for fldISIN,fldDate,fldYTMBid,fldPXCleanBid,fldPXDirtyBid in zip(df_Bondprice['Ticker'].astype(str), df_Bondprice['Date'].astype(str),df_Bondprice['YLD_YTM_BID'].astype(str),df_Bondprice['PX_BID'].astype(str),df_Bondprice['PX_DIRTY_BID'].astype(str))]
    cursor.execute(strSQLInsert)
    sql_con.commit()



def LoadNewBond():
    '''Load new bonds from file in catalog C:\FIM_FILE\StaticBondData'''
    #*************************** All fixed Coupon Bonds
    # Files in catalog C:\FIM_FILE\StaticBondData
    path="C:\FIM_FILE\StaticBondData"
    file_list= ["C:\FIM_FILE\StaticBondData\\" + x for x in os.listdir(path) if x.endswith(".csv")]
    #print(file_list)
    #print(os.stat(file_list[3]).st_size == 0)
    #**Load all bonds in all files
    cursor=sql_con.cursor()
    df_list=[]
    for bondfile in file_list:
        if not os.stat(bondfile).st_size == 0:
            bondDf=pd.read_csv(bondfile,skiprows=1,sep='|', engine='python')
            df_list.append(bondDf)
    if not len(df_list) == 0:
        bondDf=pd.concat([pd.DataFrame(x) for x in df_list])
        #cursor=sql_con.cursor()
        #******** Delete bonds before insert
        cusipList=bondDf["fldCUSIP8"].tolist()
        cusipList="','".join(str(x) for x in cusipList)
        cusipList='(\'' + cusipList + '\')'
        strSQLDelete='DELETE FROM tblFIMBond WHERE fldCUSIP8 IN ' + cusipList
        cursor.execute(strSQLDelete)
        sql_con.commit()
        #******** insert FIX bonds
        InsertData = list(zip(bondDf['fldCUSIP8'].astype(str),bondDf['fldISIN'].astype(str),bondDf['fldTicker'].astype(str),bondDf['fldSecName'].astype(str),bondDf['fldIssueDate'].astype(str)
        ,bondDf['fldMaturity'].astype(str),bondDf['fldCPN'].astype(float),bondDf['fldIntAccDate'].astype(str),bondDf['fldFirstCPNDate'].astype(str),bondDf['fldCalcMaturity'].astype(str),bondDf['fldRank'].astype(str),bondDf['fldCurrency'].astype(str)))
        strSQL=";INSERT INTO tblFIMBond VALUES ".join(str(x) for x in InsertData)
        strSQL="SET NOCOUNT ON INSERT INTO tblFIMBond VALUES " + strSQL
        cursor.execute(strSQL)
        sql_con.commit()
    #*************************** All FRN Bonds
    path="C:\FIM_FILE\StaticBondData\FRN"
    file_list= ["C:\FIM_FILE\StaticBondData\FRN\\" + x for x in os.listdir(path) if x.endswith(".csv")]
    df_list=[]
    for bondfile in file_list:
        if not os.stat(bondfile).st_size == 0:
            bondDf=pd.read_csv(bondfile,skiprows=1,sep='|', engine='python')
            df_list.append(bondDf)
    if not len(df_list) == 0:
        bondDf=pd.concat([pd.DataFrame(x) for x in df_list])
        #cursor=sql_con.cursor()
        #******** Delete bonds before insert
        cusipList=bondDf["fldCUSIP8"].tolist()
        cusipList="','".join(str(x) for x in cusipList)
        cusipList='(\'' + cusipList + '\')'
        strSQLDelete='DELETE FROM tblFIMBondFRN WHERE fldCUSIP8 IN ' + cusipList
        cursor.execute(strSQLDelete)
        sql_con.commit()
        #******** insert FRN bonds
        InsertData = list(zip(bondDf['fldCUSIP8'].astype(str),bondDf['fldISIN'].astype(str),bondDf['fldTicker'].astype(str),bondDf['fldSecName'].astype(str),bondDf['fldIssueDate'].astype(str)
        ,bondDf['fldMaturity'].astype(str),bondDf['fldFltSpread'].astype(float),bondDf['fldIntAccDate'].astype(str),bondDf['fldFirstCPNDate'].astype(str),bondDf['fldCalcMaturity'].astype(str),bondDf['fldRank'].astype(str),bondDf['fldCurrency'].astype(str)))
        strSQL=";INSERT INTO tblFIMBondFRN VALUES ".join(str(x) for x in InsertData)
        strSQL="SET NOCOUNT ON INSERT INTO tblFIMBondFRN VALUES " + strSQL
        cursor.execute(strSQL)
        sql_con.commit()
    
    sql_con.close()

def  CalcYieldCurveHSYTM(YC_list=['SGB_NOM_HSYTM'],startdate='2019-04-15',enddate='2019-04-30'):
    '''Create selected yield curves between startDate and endDate '''
    cursor=sql_con.cursor()
    for iYC in np.arange(len(YC_list)):
        sqlQuery='SELECT fldBBTicker,fldSecRank,fldShortRate,fldShortRateBBField,fldCurrency FROM tblFIMYCTemplateBond WHERE fldName=\''+ YC_list[iYC] + '\''
        df_YCTemplate=pd.read_sql(sqlQuery, sql_con)
        #*********** Get all bonds and prices
        sqlInsert=""
        strBondTicker=df_YCTemplate.iloc[0]['fldBBTicker']
        strSecRank=df_YCTemplate.iloc[0]['fldSecRank']
        strCurrency=df_YCTemplate.iloc[0]['fldCurrency']
        strR0_BBTicker=df_YCTemplate.iloc[0]['fldShortRate']
        strR0_BBField=df_YCTemplate.iloc[0]['fldShortRateBBField']
        sqlQuery = "SELECT tblFIMBondPrices.fldDate,tblFIMBond.fldSecName,tblFIMBond.fldISIN,tblFIMBond.fldMaturity,tblFIMBondPrices.fldYTMBid FROM tblFIMBondPrices INNER JOIN tblFIMBond ON tblFIMBondPrices.fldISIN = tblFIMBond.fldISIN WHERE tblFIMBondPrices.fldDate BETWEEN CONVERT(DATETIME, '" + startdate + "',120) AND CONVERT(DATETIME, '" + enddate + "',120) AND tblFIMBond.fldTicker='" + strBondTicker + "' AND tblFIMBond.fldRank in " + strSecRank + " AND tblFIMBond.fldCurrency='" + strCurrency + "'  ORDER BY tblFIMBondPrices.fldDate,tblFIMBond.fldMaturity;"
        df_BondData=pd.read_sql(sqlQuery, sql_con)
        df_BondData['fldDate'] = pd.to_datetime(df_BondData['fldDate'])
        df_BondData['fldMaturity'] = pd.to_datetime(df_BondData['fldMaturity'])
        df_BondData['T2M']=df_BondData['fldMaturity'] - df_BondData['fldDate']
        df_BondData['T2M']=df_BondData['T2M']/np.timedelta64(1,'Y')
        #**** Get all short rates
        sqlQuery = "SELECT fldDate,fldValue FROM tblFIMShortRatePrices WHERE fldDate BETWEEN CONVERT(DATETIME, '" + startdate + "',120) AND CONVERT(DATETIME, '" + enddate  + "',120) AND fldShortRate='" + strR0_BBTicker + "' AND fldShortRateBBField='" + strR0_BBField + "' ORDER BY fldDate;"
        df_ShortRates=pd.read_sql(sqlQuery, sql_con)
        df_ShortRates['fldDate'] = pd.to_datetime(df_ShortRates['fldDate'])
        #***** For all dates create a yield curve
        total_dayrows=df_ShortRates.shape[0]
        for iDay in np.arange(total_dayrows):
            df_BondDataDay=df_BondData.loc[df_BondData['fldDate'] == df_ShortRates.iloc[iDay]['fldDate']]   
            bondt2m=df_BondDataDay['T2M'].to_numpy()
            bondytm=df_BondDataDay['fldYTMBid'].to_numpy()
            bondytm=bondytm/100.0
            curve_t2m=np.zeros(14,np.float)
            curve_ytm=np.zeros(14,np.float)
            yt0=df_ShortRates.iloc[iDay]['fldValue'].astype(float)
            yt0=yt0/100.0
            curve_t2m,curve_ytm=steff_funclib.hsytm_func(curve_t2m,curve_ytm,yt0,bondt2m,bondytm)
            for i in np.arange(14):
                sqlInsert=sqlInsert + "INSERT INTO tblFIMYieldCurveData VALUES (\'" + df_ShortRates.iloc[iDay]['fldDate'].strftime('%Y-%m-%d') + "\','" + YC_list[iYC] + "\','" + str(curve_t2m[i].astype(int)) +"Y\'," + str(curve_ytm[i]) + ");"
                
        #************* Delete and insert to database
        sqlDelete="DELETE FROM tblFIMYieldCurveData WHERE fldName='" + YC_list[iYC] + "' AND fldDate BETWEEN CONVERT(DATETIME, '" + startdate + "',120) AND CONVERT(DATETIME, '" + enddate  + "',120);"
        cursor.execute(sqlDelete)
        sql_con.commit()
        sqlInsert="SET NOCOUNT ON " + sqlInsert
        cursor.execute(sqlInsert)
        sql_con.commit()
    sql_con.close()
    
#***************************************************** RUN PROGRAMS
#******************************************************************

#Run Yield Curve
'''
YC_list=['SGB_NOM_HSYTM','DBR_NOM_HSYTM','UST_NOM_HSYTM']
startdate='2018-10-01'
enddate='2019-05-03'
CalcYieldCurveHSYTM(YC_list,startdate,enddate)
'''


#LoadNewBond()



#******************** LOAD FIX COUPON BONDS

debtticker_list=['DBR','SGB','T','FASTIG','KOMINS','NDASS','SEB','SHBASS','SWEDA','TELIAS']
#debtticker_list=['NDASS']
startdate='20190502'
enddate='20190503'
#LoadBondQuotes_fix_from_debt_ticker(debtticker_list,startdate,enddate) 

#isin_list=['SE0001149311']
#LoadBondQuotes_fix_from_isin(isin_list,startdate,enddate)



#******************** LOAD short rates
#startdate='20190420'
#enddate='20190430'
#LoadShortRatesQuotes(startdate,enddate)

#startdate='20190401'
#enddate='20190421'
#LoadShortRatesQuotes_swapcurve(startdate,enddate)

#startdate='20190401'
#enddate='20190421'
#LoadSwapQuotes(startdate,enddate)



#YC_Name='SGB_NOM_HSYTM'
#calcdate1='20181231'
#calcdate2='20190503'
#print(GetHSYTM_YC(YC_Name,calcdate2))

#steff_funpltlib.two_yieldgraph_bar(GetHSYTM_YC(YC_Name,calcdate1),GetHSYTM_YC(YC_Name,calcdate2),'2018-12-31','2019-05-03','Sweden Government Yield Curve')

#************************** GET DATA to File
#sqlQuery='SELECT fldDate,fldName,fldYCParameter,fldValue FROM tblFIMYieldCurveData'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\FIM_YC_Data.csv")

#sqlQuery='SELECT fldName,fldMethod,fldCurrency,fldShortRate,fldShortRateBBField FROM tblFIMYCTemplateSwap'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\YCTemplateSwap.csv")

#sqlQuery='SELECT fldName,fldMethod,fldBBTicker,fldSecRank,fldCPNType,fldReferenceYC,fldCurrency FROM tblFIMYCTemplateFilter'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\YCTemplateFilter.csv")

#sqlQuery='SELECT fldName,fldMethod,fldBBTicker,fldSecRank,fldShortRate,fldShortRateBBField,fldCurrency FROM tblFIMYCTemplateBond'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\YCTemplateBond.csv")

#sqlQuery='SELECT fldBBCode,fldDate,fldValue FROM tblFIMSwapPrices'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\SwapPrices.csv")

#sqlQuery='SELECT fldShortRate,fldShortRateBBField,fldDate,fldValue FROM tblFIMShortRatePrices'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\ShortRatePrices.csv")

#sqlQuery='SELECT fldCUSIP8,fldDate,fldDMBid,fldPXCleanBid,fldPXDirtyBid FROM tblFIMBondPricesFRN'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\BondPricesFRN.csv")

#sqlQuery='SELECT fldISIN,fldDate,fldYTMBid,fldPXCleanBid,fldPXDirtyBid FROM tblFIMBondPrices'
#df_YC_Data=pd.read_sql(sqlQuery, sql_con)
#df_YC_Data.to_csv("C:\Code\FIM_DB\BondPrices.csv")


'''
df_YC_Data=pd.read_csv("C:\FIM_FILE\Database_Data\FIM_YC_Data.csv")
InsertData = list(zip(df_YC_Data['fldDate'].astype(str),df_YC_Data['fldName'].astype(str),df_YC_Data['fldYCParameter'].astype(str),df_YC_Data['fldValue'].astype(float)))

cursor=sql_con.cursor()
#cursor.fast_executemany = True  # new in pyodbc 4.0.19
cursor.executemany("INSERT INTO test_tblFIMYieldCurveData (fldDate,fldName,fldYCParameter,fldValue) VALUES(?, ?, ?, ?)",InsertData)
sql_con.commit()
sql_con.close()


'''

'''
cursor=sql_con.cursor()

df_YC_Data=pd.read_csv("C:\FIM_FILE\Database_Data\FIM_YC_Data.csv")
strSQL=""
#for i in np.arange(0,50000):
strSQL=["INSERT INTO test_tblFIMYieldCurveData VALUES('" + df_YC_Data.at[i,'fldDate'] + "','" + df_YC_Data.at[i,'fldName'] + "','" + df_YC_Data.at[i,'fldYCParameter'] + "'," + str(round(df_YC_Data.at[i,'fldValue'].astype(float),14)) + ")" for i in np.arange(0,50000)]

strSQL=";".join(str(x) for x in strSQL)
strSQL="SET NOCOUNT ON "+strSQL
cursor.execute(strSQL)
sql_con.commit()
sql_con.close()


#f=open("C:\Code\FIM_DB\SQL_Insert.txt","w")
#f.write(strSQL)
#f.close


'''



















