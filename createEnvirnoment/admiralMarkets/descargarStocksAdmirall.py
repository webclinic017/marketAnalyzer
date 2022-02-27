#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  7 16:48:10 2021

@author: manuel
"""
from datetime import datetime

import urllib.request as request
import urllib.parse
import json
import os
import pandas as pd
import numpy as np
import configparser
import MetaTrader5 as mt5
pd.options.mode.chained_assignment = None
config = configparser.ConfigParser()
config.read('../../config.properties')
accountNumber =int( config.get('ADMIRAL_MARKETS', 'account_number'))
password = config.get('ADMIRAL_MARKETS', 'password')

dic = {"accountNumber":accountNumber, "password": password }

if not mt5.initialize():
    print("initialize() failed")
    mt5.shutdown()

if not mt5.login(dic["accountNumber"], server="AdmiralMarkets-Demo", password=dic["password"]):
    print("Error al conectarse a cuenta de trading")
    mt5.shutdown()
symbols1 = []
exchanges = []
currencies = []
symbols = mt5.symbols_get()
dataframe = pd.DataFrame(columns=["symbol", "exchange", "currency", "isin"])
for symbol in symbols:
    symbols1.append(symbol.name)

    exchanges.append(symbol.exchange)
    currencies.append(symbol.currency_profit)
    if symbol.name[0] == "#":
        dataframe.loc[len(dataframe)] = [symbol.name[1:],
                                         symbol.exchange, symbol.currency_profit, symbol.isin]
guia_dir = config.get('ADMIRAL_MARKETS', 'guia_dir')
guardar_dir = config.get('ADMIRAL_MARKETS', 'guardar_dir')
guia_dir = config.get('ADMIRAL_MARKETS', 'guia_dir_Windows')
guardar_dir = config.get('ADMIRAL_MARKETS', 'guardar_dir_Windows')

exchanges = pd.read_csv(guia_dir+"exchanges.csv")


exchanges.set_index(["Name"], inplace=True, drop=False)
try:
    os.mkdir(guardar_dir)
except Exception as e:
    e
try:
    os.mkdir(guardar_dir+"exchanges")
except Exception as e:
    e
for e in exchanges.index:

    code = exchanges.loc[e].Code
    stocks_del_exchange = pd.read_csv(
        guia_dir+"exchanges\\"+code+"\\stocks.csv", header=0)
    stocks_del_exchange.set_index(["Name"], inplace=True, drop=True)
    codigos = []
    
    for Code1, currency in zip(stocks_del_exchange.Isin, stocks_del_exchange.Currency):

        Code = str(Code1)

        if Code in np.array(dataframe["isin"]):

            print(Code)
            codigos.append(Code)

    stocks_del_exchange = stocks_del_exchange.loc[stocks_del_exchange["Isin"].isin(
        codigos)]
    if os.path.isfile(guardar_dir+"exchanges/"+code+"/exchanges.csv"):
        print("Stock del exchange %s" % code)
    else:
        if not stocks_del_exchange.empty:
            try:
                os.mkdir(guardar_dir+"exchanges\\"+code)
            except Exception as ef:
                ef
            stocks_del_exchange.to_csv(
                guardar_dir+"exchanges/"+code+"/exchanges.csv")
