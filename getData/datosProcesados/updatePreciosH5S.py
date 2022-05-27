#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 16:42:32 2022

@author: manuel
"""
#archivo para guardar datos en formato h5s para leerlo directamente con pandas
import sys
import pandas as pd
sys.path.append("../../analyzer/getData")
sys.path.append("../../analyzer/functions")
sys.path.append("../../writer")
sys.path.append("../../performance")
sys.path.append("..")
import bdStocks
import numpy as np
import transformationsDataframes as tD
bd=bdStocks.getData()
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlalchemy
import configparser
import time
import warnings
import datetime as dt
import bdStocks
from datetime import timedelta
config = configparser.ConfigParser()
config.read('../../config.properties')
directorio=config.get('ARCHIVOS', 'archivos_h5s_precios')
import  sqlalchemy
engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
"usuario1", "password", "localhost:33062", "stocks"), pool_recycle=3600, pool_size=5).connect()
bd=bdStocks.getData()  
sql="show tables"
tablas=bd.executeQuery(sql)
tablas=[tabla[0] for tabla in tablas]
fechaI=(dt.datetime.today()-timedelta(days=65)).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
TABLAS=[tabla for tabla in tablas if (len(tabla.split("_"))==2 and tabla.split("_")[1]=="precios") ]
#%%
for tabla in TABLAS:
    
   exchange=tabla.split("_")[0]
   if exchange!="US":
    existe=True
    try:
        filename=directorio+"precios_mensual_"+exchange
        h5s = pd.HDFStore(filename + ".h5s", "r")
        temp = h5s["data"]
        h5s.close()
       
    except Exception as e:
        print(e)
        
        existe=False
   
    print(exchange)
    try:
        tiempo1=time.time()
        if   existe:
            data=bd.executeQueryDataFrame("select fecha,stock,Adjusted_close from {}_precios  where fecha>%s order by fecha asc".format(exchange),(fechaI,))
        else:
            data=bd.executeQueryDataFrame("select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange),None)
        #data=engine.execute("select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange))
        data["fecha"]=pd.to_datetime(data["fecha"])
        data=data.groupby([data["stock"],data["fecha"].dt.year,data["fecha"].dt.month],as_index=False).last()
        data["fecha"]=data["fecha"].transform(lambda x:x.replace(day=1))
        data.set_index(["fecha","stock"],inplace=True)
        data=data[~data.index.duplicated(keep='last')]
        print(sys.getsizeof(data)/1024**2)
       
        print(len(data))
       
        
        if existe:
            
            temp=temp.loc[temp.index.get_level_values(0)<=fechaI]
            print(temp.tail())
            print(data.head())
            data=pd.concat([temp,data],axis=0)
            data=data[~data.index.duplicated(keep='last')]
            data.sort_index(inplace=True)
            
        
        #%%
        print("------------------------")
        filename=directorio+"precios_mensual_"+exchange
        
        h5s = pd.HDFStore(filename + ".h5s", "w")
        h5s["data"] = data
        h5s .close()
    except Exception as e:
        print(e)
    

   
    #%%

