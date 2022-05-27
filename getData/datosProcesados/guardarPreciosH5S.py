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
import warnings
import datetime as dt
import bdStocks
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

    
TABLAS=[tabla for tabla in tablas if (len(tabla.split("_"))==2 and tabla.split("_")[1]=="precios") ]
#%%
for tabla in TABLAS:
    exchange=tabla.split("_")[0]
   
    print(exchange)
    import time
    tiempo1=time.time()
    data=bd.executeQueryDataFrame("select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange),None)
    #data=engine.execute("select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange))
    data["fecha"]=pd.to_datetime(data["fecha"])
    data=data.groupby([data["stock"],data["fecha"].dt.year,data["fecha"].dt.month],as_index=False).last()
    data["fecha"]=data["fecha"].transform(lambda x:x.replace(day=1))
    data.set_index(["fecha","stock"],inplace=True)
    data=data[~data.index.duplicated(keep='last')]
    tiempo2=time.time()
    
    #%%

    tiempo1=time.time()
    #datos=pd.read_sql("select fecha,Adjusted_close from {}_precios".format(exchange),engine)
    tiempo2=time.time()
    print(tiempo2-tiempo1)
    #%%
    print(directorio)
    filename=directorio+"precios_mensual_"+exchange
    
    h5s = pd.HDFStore(filename + ".h5s", "w")
    h5s["data"] = data
    try:
        h5s .close()
    except Exception as e:
        pass
    #%%
    filename=directorio+"precios_mensual_"+exchange
    h5s = pd.HDFStore(filename + ".h5s", "r")
    temp = h5s["data"]
    h5s.close()
    #%%
    print(temp.tail(10))
    print(sys.getsizeof(temp)/1024**2)
