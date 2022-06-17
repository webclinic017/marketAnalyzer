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
import os
os.chdir("../../")

from utils.database import bd_handler
pd.options.mode.chained_assignment = None
from utils.get_new_data import highlights
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import datetime as dt

fechalim=dt.date.today()-timedelta(days=1)

    


if __name__=="__main__":
   bd=bd_handler.bd_handler("stocks")

   query = "   select stocks.exchange as exchange, stocks.code  as stock from stocks " \
           "       inner join degiro on stocks.code=degiro.code and stocks.exchange=degiro.exchange " \
           "union " \
           "   select stocks.exchange as exchange, stocks.code   as stock from stocks " \
           "       inner join admiralmarkets " \
           "       on stocks.code=admiralmarkets.code and stocks.exchange=admiralmarkets.exchange " \
           "union " \
           "   select code as exchange,company as stock  from exchanges " \
           "       inner join market_data.indice_country on indice_country.country=exchanges.country " \
           "       inner join indices on indices.indice=indice_country.indice;"
   stock_list_short = bd.execute_query_dataframe(query)
   stock_list_short["action"] = stock_list_short.exchange + "_" + stock_list_short.stock


   tablas=[e[0] for e in list(bd.execute_query_dataframe("show tables",None).values) if len(e[0].split("_"))>1 and e[0].split("_")[1]=="precios"]
    
   stocks=bd.execute_query_dataframe("select code as stock,exchange   from stocks",None)
   stocks["action"]=stocks["exchange"]+"_"+stocks["stock"]

   stocks = stocks.loc[(stocks["action"].isin(stock_list_short.action.values))]
    
    
    
   for values in stocks.values:
       highlights.update_highlights(values, fechalim, bd)



   
    
