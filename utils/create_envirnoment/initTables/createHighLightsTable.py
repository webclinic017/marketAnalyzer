#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:39:09 2022

@author: manuel
"""
from eod import  EodHistoricalData
import os
os.chdir("../../../")
import configparser
import pandas as pd
from utils.database import bd_handler

config = configparser.ConfigParser()
config.read('config/config_key.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD_SECTION', 'api_key')
client = EodHistoricalData(api_token)
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
bd= bd_handler.bd_handler()
copy_table=True

columnas=[
  {
    "Field": "fecha",
    "Type": "date",
    "None": "NO",
    "Key": "PRI",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "stock",
    "Type": "varchar(100)",
    "None": "NO",
    "Key": "PRI",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "exchange",
    "Type": "varchar(100)",
    "None": "NO",
    "Key": "PRI",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "MarketCapitalization",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "MarketCapitalizationMln",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "EBITDA",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "PERatio",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "PEGRatio",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "WallStreetTargetPrice",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "BookValue",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "DividendShare",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "DividendYield",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "EarningsShare",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "EPSEstimateCurrentYear",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "EPSEstimateNextYear",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "EPSEstimateNextQuarter",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "EPSEstimateCurrentQuarter",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "MostRecentQuarter",
    "Type": "date",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "ProfitMargin",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "OperatingMarginTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "ReturnOnAssetsTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "ReturnOnEquityTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "RevenueTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "RevenuePerShareTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "QuarterlyRevenueGrowthYOY",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "GrossProfitTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "DilutedEpsTTM",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  },
  {
    "Field": "QuarterlyEarningsGrowthYOY",
    "Type": "double",
    "None": "YES",
    "Key": "",
    "Default": None,
    "Extra": ""
  }
]
def getHighLights(ticker):
    """
    Returns the fundamental data from the financial data API.  Combines the quarterly balance 
    sheet, cash flow, income statement, and earnings for a specific stock ticker.
    """
    
    resp = client.get_fundamental_equity(ticker, filter_='Highlights')
    
    resp= pd.DataFrame.from_dict(resp,orient="index")
    data=resp.transpose()
    data.index=[dt.datetime.today()]
    return data
    
if __name__=="__main__":
    if not copy_table:
        datos=getHighLights("AMZN.US")
        print(datos.columns)
        statement="create table if not exists  highlights( fecha date,stock varchar(100), exchange varchar(100),"
        for k in datos.columns:
            statement+=k+" double,"
        statement=statement+"PRIMARY KEY (fecha,stock,exchange));"
        
        print(statement)
        bd.execute(statement,None)
    else:

        statement="create table if not exists  last_highlights( "
        for k in columnas:
            statement+=k["Field"]+" "+k["Type"]+","
        statement=statement+"PRIMARY KEY (stock,exchange));"
        
        print(statement)
        bd.execute(statement,None)
    
   
    
