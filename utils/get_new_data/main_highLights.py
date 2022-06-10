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
from utils.database import bd_handler
pd.options.mode.chained_assignment = None
from utils.get_new_data.stocks_data_EOD_implementation.get_highLights import getHighLights
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
import logging, logging.config
from logging import getLogger
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
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


                

                
                
            pillado=False    
            for tabla in ["highlights","last_highlights"]:

                fecha=bd.execute_query("select fecha from {} where exchange=%s and stock=%s order by fecha desc limit 1".format(tabla),(values[1],values[0]))

                if fecha is None  or   len(fecha)==0 or fecha[0][0]<fechalim:
                    
                
                    try:
                        
                     if pillado==False:
                         datos=getHighLights(values[0]+"."+values[1])
                         pillado=True
                     
                    
                     if tabla=="highlights" or fecha is None or len(fecha)==0:
                         datos["fecha"]=dt.datetime.today()
                         datos["stock"]=values[0]
                         datos["exchange"]=values[1]
                         placeholder = ", ".join(["%s"] * len(datos))
                         execute="insert into {} ({}) values ({})".format(tabla,",".join(datos.keys()),placeholder)
                         bd.execute(execute, list(datos.values()))
                         logger.info("main_high_lights: Data inserted to table {}, exchange {}, stock {} ".format(tabla,
                                                                                                                 values[
                                                                                                                     1],
                                                                                                                 values[
                                                                                                                     0]))

                     else:
                         for key in ["stock","exchange"]:
                             if key in datos.keys():
                                 datos.pop(key)
                         placeholder="set "
                         for e in datos.keys():
                             placeholder +=e+"=%s, "
                         placeholder=placeholder [:-2]+" "
                         execute="update {} {} where stock=%s and exchange=%s ".format(tabla,placeholder)
                         datos["stock"]=values[0]
                         datos["exchange"]=values[1]
                         bd.execute(execute, list(datos.values()))
                         logger.info("main_high_lights: Data updated to table {}, exchange {}, stock {} ".format(tabla,
                                                                                                                 values[
                                                                                                                     1],
                                                                                                                 values[
                                                                                                                     0]))

                         

                 
                  

                    except HTTPError as e:
                        logger.error("main_high_lights: HTTPERROR: table {}, exchange {}, stock {}: ".format(tabla, values[1], values[0])+str(e))
                    except Exception as e:
                         logger.error(
                            "main_high_lights: ERROR: table {}, exchange {}, stock {}: ".format(tabla,values[1], values[0]) + str(e))

                         if tabla=="highlights" or fecha is None or  len(fecha)==0:
                             execute="insert into {} (fecha,stock,exchange) values (%s,%s,%s)".format(tabla)
                         else:
                              execute="update {} set fecha=%s where stock=%s and exchange=%s".format(tabla)

                         bd.execute(execute,(dt.date.today(),values[0],values[1]))

                         logger.info("main_high_lights: Incomplete data added to table {}, exchange {}, stock {} ".format(tabla,values[1], values[0]))

   
    
