#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 15:05:45 2021

@author: manuel
"""


import urllib.request as request
import argparse
from datetime import datetime, timedelta
from functools import reduce
from pangres import upsert
import sqlalchemy
import os
import pandas as pd
import numpy as np
import json
import time
from datetime import timedelta
import datetime as dt
import operator as opt
import math
from pandas.io.pytables import IndexCol
import mysql.connector
import saveFundamental_functions as saveStocks
import savePrizes_functions as savePrices
import configparser
import warnings
import sys, os


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout
sys.path.append("EOD")

from EOD.getResults import getResults
config = configparser.ConfigParser()
config.read('../config.properties')
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
HOST = config.get('DatabaseStocksSection', 'database_host')
USER = config.get('DatabaseStocksSection', 'database_user')
PASSWORD = config.get('DatabaseStocksSection', 'database_password')
PORT = config.get('DatabaseStocksSection', 'database_port')
DATABASE2 = config.get('DatabaseStocksSection', 'database_name')
TABLE2 = config.get('DatabaseStocksSection', 'database_table_name')
DAYS_UPDATE_RESULTS = int(config.get('DatabaseStocksSection', 'days_update_results'))
database_username = USER
database_password = PASSWORD
database_ip = HOST+":"+PORT
database_name = DATABASE2
data_dir = config.get('DatabaseStocksSection', 'data_dir')
engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
    database_username, database_password, database_ip, database_name), pool_recycle=3600, pool_size=5).connect()
directorioALMACENAMIENTO = config.get('DatabaseStocksSection', 'storage_dir')
mydb = mysql.connector.connect(
    host=HOST,
    user=USER,
    port=PORT,
    password=PASSWORD, database=DATABASE2
)
mycursorStocks = mydb.cursor()
FECHA1="1990-01-01"
FECHA2="2015-01-02"
fecha1 = dt.datetime.strptime(FECHA1, "%Y-%m-%d")
fecha2 =  dt.datetime.strptime(FECHA2, "%Y-%m-%d")
print("Fecha desde la que se actualizan resultados %s"%fecha1)
print("Fecha hasta la que se actualizan resultados %s"%fecha2)
sql="delete from calendarioResultados where report_date>=%s and report_date<=%s"
#mycursorStocks.execute(sql,(fecha1,fecha2))
#mydb.commit()
saveStock =   True if config.get('DatabaseStocksSection', "saveStockFundamentalsWhenUpdatingResults")=="True" else False
time_grid = pd.date_range(start=fecha1, end=fecha2, freq='10D').to_pydatetime()
print("Numero de rangos de fechas %s" % (len(time_grid)-1))
for i in range(1, len(time_grid)):
  
    fecha1_aux = dt.datetime.strftime(time_grid[i-1], "%Y-%m-%d")
    fecha2_aux = dt.datetime.strftime(time_grid[i]-timedelta(days=1), "%Y-%m-%d")
    print("Fecha inicial del rango %s %s" %(i, fecha1_aux))
    print("Fecha final del rango %s %s" %(i, fecha2_aux))
    resp = getResults(fecha1_aux, fecha2_aux)
    dataframe = pd.DataFrame(resp["earnings"])
    if len(dataframe)>0:
        dataframe=dataframe.loc[:, [
            "code", "report_date", "date", "actual", "estimate"]]
        print(dataframe.tail(10))
        dataframe["stock"] = dataframe["code"].transform(lambda x: x.split(".")[0])
        dataframe["exchange"] = dataframe["code"].transform(
            lambda x: x.split(".")[1])
        dataframe.drop("code", inplace=True, axis=1)
        # dataframe=dataframe[dataframe["actual"].notna()]
    
        print("Número de resultados  en este rango %s" % len(dataframe))
        dataframe.to_sql("calendarioResultados", engine,
                         if_exists="append", chunksize=1000, index=False)
    