#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:48:55 2022

@author: manuel
"""
import time
import configparser
import os
import urllib.parse
import urllib.request as request
import yfinance as yf
import numpy as np
import sys
import json
import operator
import pandas as pd
from datetime import timedelta
import pytz as tz
from investpy import stocks as st
from investpy import news
import math
import mysql.connector
from time import sleep
import argparse
import sqlalchemy
from pangres import upsert
import requests
from io import StringIO
import requests_cache
import datetime
import datetime as dt
from EOD.getFundamentals import getFundamentals
PRINT = False

pd.options.mode.chained_assignment = None
config = configparser.ConfigParser()
config.read('../config.properties')
HOST = config.get('DatabaseStocksSection', 'database_host')
USER = config.get('DatabaseStocksSection', 'database_user')
PASSWORD = config.get('DatabaseStocksSection', 'database_password')
PORT = config.get('DatabaseStocksSection', 'database_port')
DATABASE2 = config.get('DatabaseStocksSection', 'database_name')
TABLE2 = config.get('DatabaseStocksSection', 'database_table_name')
DAYS_UPDATE = int(config.get('DatabaseStocksSection', 'days_update_fundamental'))
DAYS_UPDATE = -1
data_dir = config.get('DatabaseStocksSection', 'data_dir')
database_username = USER
database_password = PASSWORD
database_ip = HOST+":"+PORT
database_name = DATABASE2
engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
    database_username, database_password, database_ip, database_name), pool_recycle=3600, pool_size=5).connect()
directorioALMACENAMIENTO = config.get(
    'DatabaseStocksSection', 'fundamental_storage_dir')
mydbStocks = mysql.connector.connect(
    host=HOST,
    user=USER,
    port=PORT,
    password=PASSWORD, database=DATABASE2
)
mycursorStocks = mydbStocks.cursor()
dataframe = pd.read_csv(data_dir+"exchanges.csv")


dataframe.set_index(["Name"], inplace=True, drop=True)

exchanges = dataframe
def init():
     return mycursorStocks,mydbStocks,data_dir


def UpdateDatosEnBD(precios, exchange, stock):

    an_array = np.full((len((precios))), exchange)
    precios["exchange"] = an_array
    an_array = np.full((len((precios))), stock)
    precios["stock"] = an_array
    precios.index.names = ["fecha"]
    precios.to_sql(exchange+"_fundamental", engine,
                   if_exists="append", chunksize=1000)

  

    mydbStocks.commit()




def comprobarSiExisteExchange(exchange):
    sql = "SHOW TABLES LIKE %s;"

    mycursorStocks.execute(sql, (exchange+"_fundamental",))
    codigo = mycursorStocks.fetchone()
    mydbStocks.commit()
    # print(codigo)

    if codigo is None:

        precios = getFundamentals(
            "AMZN.US")
        u = precios
        u.drop(labels="currency_symbol", inplace=True, axis=1)
        cols = " double,".join([str(i) for i in u.columns.tolist()])
        cols = "fecha date,"+cols
        cols = cols + \
            " double,stock varchar(100),exchange varchar(100), PRIMARY KEY (stock,exchange,fecha));"

        sql = "CREATE TABLE " + exchange+"_fundamental"+" ("+cols
        # print(sql)
        mydbStocks.cursor().execute(sql)
        mydbStocks.commit()
        # the connection is not autocommitted by default, so we must commit to save our # changes

    try:
        os.mkdir(directorioALMACENAMIENTO+exchange)
    except Exception as e:
        e
