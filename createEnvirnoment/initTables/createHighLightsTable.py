#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:39:09 2022

@author: manuel
"""
from eod import  EodHistoricalData
import configparser
import pandas as pd
import sys
sys.path.append("../../analyzer/getData")
import bdStocks
config = configparser.ConfigParser()
config.read('../../config.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD_SECTION', 'api_key')
client = EodHistoricalData(api_token)
import pandas as pd
from functools import reduce
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
bd=bdStocks.getData()
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
    datos=getHighLights("AMZN.US")
    print(datos.columns)
    statement="create table highlights( fecha date,stock varchar(100), exchange varchar(100),"
    for k in datos.columns:
        statement+=k+" double,"
    statement=statement+"PRIMARY KEY (fecha,stock,exchange));"
    
    print(statement)
    bd.execute(statement,None)
    
   
    