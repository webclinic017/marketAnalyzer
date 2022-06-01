#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:15:00 2022

@author: manuel
"""
import configparser
import datetime as dt
import os
import sys
import warnings
from datetime import timedelta
import mysql.connector
import numpy as np
import pandas as pd
import sqlalchemy
from utils.get_new_data import db_functions
from utils.get_new_data.EOD.getPrizes import get_eod_data

PRINT = False
config = configparser.ConfigParser()
config.read('config/config.properties')
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
DAYS_UPDATE = int(config.get('DatabaseStocksSection', 'days_update'))
mycursorStocks, mydbStocks, data_dir,engine,  directorio_almacenamiento =db_functions.init('DatabaseStocksSection')



def guardarDatosEnBD(precios, exchange, stock):
    an_array = np.full((len((precios))), exchange)
    precios["exchange"] = an_array
    an_array = np.full((len((precios))), stock)
    precios["stock"] = an_array
    precios.index.names = ["fecha"]
    precios.to_sql(exchange + "_precios", engine,
                   if_exists="append", chunksize=1000)


def comprobarSiExisteExchange(exchange):
    sql = "SHOW TABLES LIKE %s;"

    mycursorStocks.execute(sql, (exchange + "_precios",))
    codigo = mycursorStocks.fetchone()
    mydbStocks.commit()
    # print(codigo)
    dataframe1 = pd.read_csv(data_dir + "exchanges/" +
                             exchange + "/stocks.csv", dtype=str)
    dataframe1.set_index("Name", inplace=True, drop=True)
    if codigo is None:
        precios = get_eod_data(
            "AMZN.US")
        u = precios
        cols = " double,".join([str(i) for i in u.columns.tolist()])
        cols = "fecha date," + cols
        cols = cols + \
               " double,stock varchar(100),exchange varchar(100), PRIMARY KEY (stock,exchange,fecha));"

        sql = "CREATE TABLE " + exchange + "_precios" + " (" + cols
        # print(sql)
        mydbStocks.cursor().execute(sql)
        # the connection is not autocommitted by default, so we must commit to save our # changes
        mydbStocks.commit()

    try:
        os.mkdir(directorio_almacenamiento + exchange)
    except Exception as e:
        e


def cargarPrecios(stock, exchange):
    name_archivo = directorio_almacenamiento + exchange + "/" + stock + ".csv"
    fecha2 = None
    prize_df = None
    if os.path.isfile(name_archivo):
        prize_df = pd.read_csv(name_archivo, index_col=0)
        if not prize_df.empty:
            prize_df.index = pd.to_datetime(prize_df.index)
            fecha2 = prize_df.index[-1]
            fecha2 = dt.datetime(fecha2.year, fecha2.month, fecha2.day)

    sql = "select fecha from  {}_precios where stock=%s order by fecha desc limit 1".format(
        exchange)
    mycursorStocks.execute(sql, (stock,))
    fecha = pd.DataFrame(mycursorStocks.fetchone())
    mydbStocks.commit()

    if fecha.empty or fecha2 is None:
        if fecha.empty:
            fecha = None
        precios = get_eod_data(
            stock + "." + exchange)

    if not fecha is None:
        fecha = fecha.iloc[0, 0]

        fecha = dt.datetime(fecha.year, fecha.month, fecha.day)
    if not fecha is None and not fecha2 is None:
        minimo = min(dt.datetime(fecha.year, fecha.month, fecha.day), fecha2)
        if minimo >= dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=DAYS_UPDATE):
            print("Stock %s_%s ya actualizado" % (exchange, stock))
            return

        minimo = minimo.strftime("%Y-%m-%d")

        precios = get_eod_data(
            stock + "." + exchange, date=minimo)
    if precios is not None:
        precios.index = pd.to_datetime(precios.index)

        if not precios.empty:
            if not fecha2 is None:
                precios1 = precios.loc[precios.index > fecha2]

                prize_df = pd.concat([prize_df, precios1])
                prize_df.sort_index(ascending=True, inplace=True)
            else:
                prize_df = precios
                prize_df.sort_index(ascending=True, inplace=True)

            prize_df.to_csv(name_archivo)
            if not fecha is None:
                precios1 = precios.loc[precios.index > fecha]

            else:
                precios1 = precios

            guardarDatosEnBD(precios1, exchange, stock)
            print("Stock %s_%s no esta actualizado" % (exchange, stock))

        if not fecha is None and not fecha2 is None and PRINT:
            print("Fecha de BD %s" % fecha)
            print("Fecha de csv %s" % fecha2)
