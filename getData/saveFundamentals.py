#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  6 22:57:38 2022

@author: manuel
"""
from EOD.getFundamentals import getFundamentals
import saveFundamental_functions
import datetime as dt
from datetime import timedelta
import configparser
import numpy as np
import pandas as pd
import savePrizes_functions
config = configparser.ConfigParser()
config.read('../config.properties')
directorioALMACENAMIENTO = config.get('DatabaseStocksSection', 'fundamental_storage_dir')
if __name__ == "__main__":
    mycursorStocks, mydbStocks, data_dir = saveFundamental_functions.init()
    sql="show tables"
    mycursorStocks.execute(sql)
    tablas=mycursorStocks.fetchall()
    mydbStocks.commit()
    tablas=[tabla[0] for tabla in tablas]
    
    
    TABLAS=[tabla for tabla in tablas if (len(tabla.split("_"))==2 and tabla.split("_")[1]=="fundamental") ]
    
    print(TABLAS)
    kk=0
    ex_loss=0
    for tabla in TABLAS:

            print(tabla)
            if kk==1:
                print(ex_loss)
            exchange=tabla.split("_")[0]
            sql = " select fund.stock as stock,fecha,date,exchange from ( \
                        select stock,max(fecha) as fecha from {} group by stock) as fund \
                            inner join \
                        (select stock,max(date) as date,exchange from calendarioResultados \
                                     where report_date<%s group by stock,exchange) as c \
                            on  fund.stock=c.stock where c.exchange=%s and date>fecha;".format(tabla)
            mycursorStocks.execute(sql,(dt.datetime.today(),exchange))
            stocks=pd.DataFrame(mycursorStocks.fetchall())
            if len(stocks)>0:
                stocks.columns=  mycursorStocks.column_names
            mydbStocks.commit()
            if len(stocks)>0:
                stocks.columns=  mycursorStocks.column_names
               
                stocks["fecha"]=pd.to_datetime(stocks.fecha)
                stocks["date"]=pd.to_datetime(stocks.date)
                print(stocks)
                for i in stocks.index:
                    try:
                                fila=stocks.loc[i]
                                stock=fila["stock"]
                               
                                fecha=fila["fecha"]
                                date=fila["date"]
                               
                                precios = getFundamentals(
                                stock+"."+exchange)
                                name_archivo = directorioALMACENAMIENTO+exchange+"/"+stock+".csv"
                                if not precios is None:
                                    if "currency_symbol" in precios.columns:
                                        precios.drop(labels="currency_symbol",inplace=True,axis=1)
                                    precios = precios.dropna( axis=0, 
                                            how='all')
                                    precios.index=pd.to_datetime(precios.index)
                                    if not precios.empty:
                                            prize_df = precios
                                            prize_df.sort_index(ascending=True, inplace=True)
                                            prize_df.to_csv(name_archivo)  
                                            precios1 = precios.loc[precios.index>fecha]
                                            if len(precios1)>0:
                                                saveFundamental_functions.UpdateDatosEnBD(precios1,exchange,stock)
                                                print(exchange,stock)
                                               
                                            else:
                                                print("Empty")
                                            savePrizes_functions.cargarPrecios(stock, exchange)
                    except Exception as e:
                        if kk==0:
                            ex_loss=exchange
                        kk=1
                        print(e)
                 
                