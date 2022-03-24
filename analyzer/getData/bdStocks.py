#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 13:47:36 2022

@author: manuel
"""

import numpy as np
import configparser
import warnings
import pandas as pd
import mysql.connector
import time
import sys
sys.path.append("../../performance")
sys.path.append("../../writer")
import writer
import os
retval=os.getcwd()
os.chdir(os.path.dirname(os.path.abspath(__file__)))
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
PERFORMANCE=False
mydbStocks = mysql.connector.connect(
    host=HOST,
    user=USER,
    port=PORT,
    password=PASSWORD, database=DATABASE
)
mycursorStocks = mydbStocks.cursor()
os.chdir(retval)

class getData:
    def getStocks(self,exchange,broker=None):
        if broker is None:
            stocks_del_exchange=pd.read_csv(data_dir +"exchanges/"+exchange+"/stocks.csv",index_col=1)
            return list(stocks_del_exchange.index)
        else:
            if exchange!='US':
                sql="select distinct(code) from {} where exchange=%s".format(broker)
                mycursorStocks.execute(sql,(exchange,))
            else:
                 sql="select distinct(code) from {} where exchange='NASDAQ' or exchange='NYSE'".format(broker)
                 mycursorStocks.execute(sql)
                
            
            stocks=list( mycursorStocks.fetchall())
            mydbStocks.commit()
            return [ e[0] for e in stocks]
       
    def getDataByExchange(self,tipo:str, exchange: str, broker: str = None, bd: bool = False,columnas=None):

        import os
        if PERFORMANCE:
            timer = TimeMeasure()
        if not bd:

            if broker is None:
                if tipo=="precios":
                    directorio = prizes_storage_dir+exchange+"/"
                elif tipo=="fundamental":
                     directorio = fundamental_storage_dir+exchange+"/"
                files = os.listdir(directorio)
                dataframe =  self.__read_csv(files[0], directorio)
                
                files = files[1:]
                for file in files:
                    dataframe1 = self.__read_csv(file, directorio)
                    dataframe = pd.concat([dataframe, dataframe1], axis=0)
            acceso="CSV"
           

        else:
            if tipo=="precios":
                sql = "select * from {}_precios;".format(exchange)
            elif tipo=="fundamental":
                sql = "select * from {}_fundamental;".format(exchange)
            dataframe=self.__read_bd(sql)
            acceso="BD"
        if PERFORMANCE:
            writer.write(acceso+" access time to get "+tipo+" "+str(timer.getTime()),
                             log_dir_rendimiento)
            writer.write("Object size ({} data)".format(tipo)+str(sys.getsizeof(dataframe) /
                     1000000)+" MB", log_dir_rendimiento)
        dataframe.rename_axis(index=["fecha"],inplace=True)
        if columnas is not None:
            return dataframe.loc[:,columnas]
        return dataframe
    def getDataByStock(self,tipo:str, exchange: str,stock:str, broker: str = None, bd: bool = False,columnas=None):

        import os
        if PERFORMANCE:
            timer = TimeMeasure()
        if not bd:

            if broker is None:
                if tipo=="precios":
                    directorio = prizes_storage_dir+exchange+"/"
                elif tipo=="fundamental":
                     directorio = fundamental_storage_dir+exchange+"/"
                file=stock+".csv"
                dataframe =  self.__read_csv(file, directorio)
                
              
            acceso="CSV"
        else:
            if tipo=="precios":
                sql = "select * from {}_precios where stock=%s;".format(exchange)
            elif tipo=="fundamental":
                sql = "select * from {}_fundamental where stock=%s;".format(exchange)
            dataframe=self.__read_bd_withParams(sql,(stock,))
            acceso="BD"
        if PERFORMANCE:
            writer.write(acceso+" access time to get {} ".format(tipo)+str(timer.getTime()),
                             log_dir_rendimiento)
            writer.write("Object size ({} data)".format(tipo)+str(sys.getsizeof(dataframe) /
                     1000000)+" MB", log_dir_rendimiento)
        dataframe.rename_axis(index=["fecha"],inplace=True)
        if columnas is not None:
            return dataframe.loc[:,columnas]
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
    def __read_bd_withParams(self,sql,params):
            mycursorStocks.execute(sql,params)
            dataframe = pd.DataFrame(mycursorStocks.fetchall())
            dataframe.columns = mycursorStocks.column_names

            dataframe.index = pd.to_datetime(dataframe["fecha"])
            dataframe.drop(["exchange", "fecha"], axis=1, inplace=True)
            dataframe = dataframe.drop_duplicates().sort_index(level=0, ascending=True)
            mydbStocks.commit()
            return dataframe
    def getIndexPrizes(self,index):
        sql="select * from preciosIndices where indice=%s"
        mycursorStocks.execute(sql,(index,))
        dataframe = pd.DataFrame(mycursorStocks.fetchall())
        dataframe.columns = mycursorStocks.column_names
        dataframe.index = pd.to_datetime(dataframe["Date"])
        dataframe = dataframe.drop_duplicates().sort_index(level=0, ascending=True)
        mydbStocks.commit()
        dataframe.rename_axis(index=["fecha"],inplace=True)
        return dataframe
        
    def getSectors(self,exchange):
        if PERFORMANCE:
            timer = TimeMeasure()
        sql="select stock,sector from sectors where exchange=%s"
        mycursorStocks.execute(sql,(exchange,))
        data=(mycursorStocks.fetchall())
        mydbStocks.commit()
        
        if PERFORMANCE:
            writer.write("BD access time to sectors "+str(timer.getTime()),
                     log_dir_rendimiento)
        return dict(data)
    
        
    def getDescriptions(self,exchange):
        if PERFORMANCE:
            timer = TimeMeasure()
        sql="select stock,description from descriptions where exchange=%s"
        mycursorStocks.execute(sql,(exchange,))
        data=(mycursorStocks.fetchall())
        mydbStocks.commit()
        
        if PERFORMANCE:
            writer.write("BD access time to sectors "+str(timer.getTime()),
                     log_dir_rendimiento)
        return dict(data)
    def obtenerStocksStrategy(self,filt,tipo,columnas=None):
        broker=None
        stocks=[]
        dataframe=None
        stocksAEliminar=[]
        if "broker" in filt.keys():
            broker=filt["broker"]
        for exchange in filt["exchanges"]:
                print(exchange)
                sector=None
                if "sector" in filt.keys():
                 
                         sector=self.getSectors(exchange)
                         aux1=self.getStocks(exchange,broker=broker)
                         aux=[e for e,valor in sector.items() if valor!= None and valor in filt["sector"] and e in aux1]
                         
                else:
                    aux=self.getStocks(exchange,broker=broker)
                for stock in aux:
                       
                        try:
                            dataframe1=self.getDataByStock(tipo,exchange,stock,columnas=columnas)
                            dataframe1["stock"]=stock
                            dataframe1["exchange"]=exchange
                            if "sector" in filt.keys():
                                   
                                    dataframe1["sector"]=dataframe1["stock"].transform(lambda t:sector[t])
                            if dataframe is None:
                                dataframe=dataframe1
                            else:
                                dataframe=pd.concat([dataframe,dataframe1])
                            
                        except Exception as e:
                            stocksAEliminar.append(stock)
                            
                aux=list(set(aux)-set(stocksAEliminar))            
                stocks+=[exchange+"_"+e for e in aux]  
        
                
        print(stocks)
        return dataframe,stocks
if __name__=="__main__":
        
    bd=  getData()
    filt={"broker":"degiro","exchanges":["US"],"sector":["Healthcare"]}
    dataframe=bd.obtenerStocksStrategy(filt,"fundamental",columnas=["netIncome"])
