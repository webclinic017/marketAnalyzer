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
config = configparser.ConfigParser()
config.read('config/config_key.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD', 'api_key')
client = EodHistoricalData(api_token)
import pandas as pd
from functools import reduce
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
def getHighLights(ticker):
    """
    Returns the fundamental data from the financial data API.  Combines the quarterly balance 
    sheet, cash flow, income statement, and earnings for a specific stock ticker.
    """
    
    resp = client.get_fundamental_equity(ticker, filter_='Highlights')
    return resp
