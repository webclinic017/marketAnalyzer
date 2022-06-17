#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 17:25:05 2022

@author: manuel
"""
from eod import EodHistoricalData
import requests
import requests_cache
import pandas as pd
from io import StringIO
import datetime
import configparser
config = configparser.ConfigParser()
config.read('config/config_key.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD', 'api_key')
client = EodHistoricalData(api_token)

def get_splits(fecha1, fecha2):
    
    
    resp = client.get_calendar_splits(from_=fecha1, to=fecha2)
    return resp
