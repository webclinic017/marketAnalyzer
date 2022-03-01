#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 13:47:36 2022

@author: manuel
"""


import configparser
import warnings
import pandas as pd
import mysql.connector
import time
import sys
sys.path.append("../../performance")
sys.path.append("../../writer")
import writer
from timeMeasure import TimeMeasure
config = configparser.ConfigParser()
config.read('../../config.properties')
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
HOST = config.get('DatabaseStocksSection', 'database_host')
USER = config.get('DatabaseStocksSection', 'database_user')
PASSWORD = config.get('DatabaseStocksSection', 'database_password')
PORT = config.get('DatabaseStocksSection', 'database_port')
DATABASE = config.get('DatabaseStocksSection', 'database_name')
data_dir = config.get('DatabaseStocksSection', 'data_dir')
log_dir_rendimiento = "../../"+config.get('LOGS_DIR', 'rendimiento')
fundamental_storage_dir = config.get(
    'DatabaseStocksSection', 'fundamental_storage_dir')
prizes_storage_dir = config.get('DatabaseStocksSection', 'storage_dir')
PERFORMANCE = True if config.get('PERFORMANCE', 'time') == "True" else False
mydbStocks = mysql.connector.connect(
    host=HOST,
    user=USER,
    port=PORT,
    password=PASSWORD, database=DATABASE
)
mycursorStocks = mydbStocks.cursor()


class getData:
    def getPrizesByExchange(self, exchange: str, broker: str = None, bd: bool = False):

        import os
        if PERFORMANCE:
            timer = TimeMeasure()
        if not bd:

            if broker is None:

                directorio = prizes_storage_dir+exchange+"/"

                files = os.listdir(directorio)
                dataframe =  self.__read_csv(files[0], directorio)
                
                files = files[1:]
                for file in files:
                    dataframe1 = self.__read_csv(file, directorio)
                    dataframe = pd.concat([dataframe, dataframe1], axis=0)

        else:
            sql = "select * from {}_precios;".format(exchange)
            dataframe=self.__read_bd(sql)
          
        writer.write("CSV access time "+str(timer.getTime()),
                     log_dir_rendimiento)
        writer.write("Object size "+str(sys.getsizeof(dataframe) /
                     1000000)+" MB", log_dir_rendimiento)
        return dataframe

    def __read_csv(self, file, directorio):

        dataframe = pd.read_csv(directorio+file, index_col=0)
        dataframe.index = pd.to_datetime(dataframe.index)
        dataframe["stock"] = file.split(".")[0]
        dataframe = dataframe.drop_duplicates().sort_index(level=0, ascending=True)
        return dataframe
    
    def __read_bd(self,sql):
            mycursorStocks.execute(sql)
            dataframe = pd.DataFrame(mycursorStocks.fetchall())
            dataframe.columns = mycursorStocks.column_names

            dataframe.index = pd.to_datetime(dataframe["fecha"])
            dataframe.drop(["exchange", "fecha"], axis=1, inplace=True)
            dataframe = dataframe.drop_duplicates().sort_index(level=0, ascending=True)
            mydbStocks.commit()
            return dataframe
        

    def getFundamentalsByExchange(self, exchange: str, broker: str = None, bd: bool = False):

        import os
        if PERFORMANCE:
            timer = TimeMeasure()
        if not bd:

            if broker is None:

                directorio = fundamental_storage_dir+exchange+"/"

                files = os.listdir(directorio)
                dataframe = self.__read_csv(files[0], directorio)
                files = files[1:]
                for file in files:
                    dataframe1 = self.__read_csv(file, directorio)
                    dataframe = pd.concat([dataframe, dataframe1], axis=0)

        else:
            sql = "select * from {}_fundamental;".format(exchange)
          
            dataframe=self.__read_bd(sql)
        writer.write("BD access time "+str(timer.getTime()),
                     log_dir_rendimiento)
        writer.write("Object size "+str(sys.getsizeof(dataframe) /
                     1000000)+" MB", log_dir_rendimiento)
        return dataframe
    def getIndexPrizes(self,index):
        sql="select * from preciosIndices where indice=%s"
        mycursorStocks.execute(sql,(index,))
        dataframe = pd.DataFrame(mycursorStocks.fetchall())
        dataframe.columns = mycursorStocks.column_names
        dataframe.index = pd.to_datetime(dataframe["Date"])
        dataframe = dataframe.drop_duplicates().sort_index(level=0, ascending=True)
        return dataframe
        


