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
from requests.exceptions import HTTPError
from utils.get_new_data import db_functions
pd.options.mode.chained_assignment = None
from utils.get_new_data.EOD.getHighLights import getHighLights
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import datetime as dt

fechalim=dt.date.today()-timedelta(days=10)

    


if __name__=="__main__":
    mycursorStocks, mydbStocks, data_dir, engine, directorio_almacenamiento = db_functions.init(
        'DatabaseStocksSection')
    palabra_clave="%energy%"
    palabra_clave2="%cybersecurity%"
    consulta1="select stock,exchange from descriptions where description like %s "
    dataframe=db_functions.executeQueryDataFrame(mycursorStocks,mydbStocks,consulta1,(palabra_clave,))
    print(dataframe)
    
    tablas=[e[0] for e in list(db_functions.executeQueryDataFrame(mycursorStocks,mydbStocks,"show tables",None).values) if len(e[0].split("_"))>1 and e[0].split("_")[1]=="precios"]
    
    stocks=db_functions.executeQueryDataFrame(mycursorStocks,mydbStocks,"select code,exchange as stock  from stocks",None)
        
    
    
    
    
    for values in stocks.values:
        if values[1] in ["US","NYSE","NASDAQ","MC","XETRA","BE","DU","F","TO","VX","AS","AT","ST","HE","PA","LSE","SA","LSE","MI"]:
            stock_nombre_numerico=True
            try:
                numero=int(values[1][0])
              
            except Exception as e:
                stock_nombre_numerico=False
                
          
            if values[1]=="NYSE" or values[1]=="NASDAQ":
                values[1]="US"
                
                
            pillado=False    
            for tabla in ["highlights","last_highlights"]:
                print(tabla)
                fecha=db_functions.executeQuery(mycursorStocks,mydbStocks,"select fecha as fecha from {} where exchange=%s and stock=%s order by fecha desc limit 1".format(tabla),(values[1],values[0]))
                print(fecha)
                if len(fecha)==0 or fecha[0][0]<fechalim:
                    
                
                    try:   
                        
                     if pillado==False:
                         datos=getHighLights.getHighLights(values[0]+"."+values[1])
                         pillado=True
                     
                    
                     if tabla=="highlights" or len(fecha)==0:
                         datos["fecha"]=dt.datetime.today()
                         datos["stock"]=values[0]
                         datos["exchange"]=values[1]
                         placeholder = ", ".join(["%s"] * len(datos))
                         execute="insert into {} ({}) values ({})".format(tabla,",".join(datos.keys()),placeholder)
                     else:
                         placeholder="set "
                         for e in datos.keys():
                             placeholder +=e+"=%s, "
                         placeholder=placeholder [:-2]+" "
                         execute="update {} {} where stock=%s and exchange=%s ".format(tabla,placeholder)
                         datos["stock"]=values[0]
                         datos["exchange"]=values[1]
                         
                     print(values)
                 
                  
                     db_functions.execute(mycursorStocks,mydbStocks,execute,list(datos.values()))
                    except HTTPError as e:
                        print(e)
                    except Exception as e:
                        
                         print(dt.date.today(),values[0],values[1])
                         if tabla=="highlights" or len(fecha)==0:
                             execute="insert into {} (fecha,stock,exchange) values (%s,%s,%s)".format(tabla)
                         else:
                              execute="update {} set fecha=%s where stock=%s and exchange=%s".format(tabla)
                             
                         db_functions.execute(mycursorStocks,mydbStocks,execute,(dt.date.today(),values[0],values[1]))
        
   
    
