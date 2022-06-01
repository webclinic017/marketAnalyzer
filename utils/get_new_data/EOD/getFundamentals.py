#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:39:09 2022

@author: manuel
"""
from eod import  EodHistoricalData
import configparser
import pandas as pd
config = configparser.ConfigParser()
config.read('config/config.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD_SECTION', 'api_key')

client = EodHistoricalData(api_token)
import pandas as pd
from functools import reduce
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
def getFundamentals(ticker):
    """
    Returns the fundamental data from the financial data API.  Combines the quarterly balance 
    sheet, cash flow, income statement, and earnings for a specific stock ticker.
    """
    
    resp = client.get_fundamental_equity(ticker, filter_='Financials')
    
    if isinstance(resp,dict):
      
        if  "Balance_Sheet" in resp.keys() and 'Cash_Flow' in resp.keys() and 'Income_Statement' in resp.keys()\
        and  isinstance(resp[ "Balance_Sheet"],dict)   and  isinstance(resp[ 'Income_Statement'],dict)\
        and  isinstance(resp[ 'Cash_Flow'],dict)   and "quarterly" in resp['Cash_Flow'].keys() and\
            "quarterly" in resp["Balance_Sheet"].keys() and    "quarterly" in resp['Income_Statement'].keys() :
        # Financials
            bal = pd.DataFrame(resp["Balance_Sheet"]["quarterly"]).T
            
            cf = pd.DataFrame(resp['Cash_Flow']['quarterly']).T
            
            inc = pd.DataFrame(resp['Income_Statement']['quarterly']).T
            
        
            
            # Merging them together
            df = reduce(
                lambda left,right: pd.merge(
                    left,
                    right,
                    left_index=True, 
                    right_index=True, 
                    how='outer',
                    suffixes=('', '_drop')
                ), 
                [bal, cf, inc]
            )
            
            # Dropping redundant date and duplicate columns
            dup_cols = [i for i in df.columns if "date" in i or "Date" in i or "_drop" in i]
            
            df = df.drop(dup_cols, axis=1)
            
            return df
        else:
            return None
    else:
        return None
