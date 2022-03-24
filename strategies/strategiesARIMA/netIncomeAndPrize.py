#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 13:24:08 2022

@author: manuel
"""
import bdStocks
import obtenerModelos
import sys
sys.path.append("../getData")
sys.path.append("../analyzer/mlflow")
sys.path.append("../analyzer/functions")

bd = bdStocks.getData()
BACKTESING = True
filt = {"broker": "degiro", "exchanges": ["XETRA"]}
import time
tiempo1=time.time()
fundamental,stocksFundamental = bd.obtenerStocksStrategy(
    filt, "fundamental", columnas=["netIncome"])
precios,stocksPrecios = bd.obtenerStocksStrategy(
    filt, "precios", columnas=["Adjusted_close"])
tiempo2=time.time()
print(tiempo2-tiempo1)
#%%
ob = obtenerModelos.ObtenerModelos()

data=bd.getDataByStock(tipo="fundamental", exchange="XETRA",stock="02M",columnas=["netIncome"])
nas,minimo,model,model2,fechaI=ob.getModelo("02M","XETRA","netIncome",data,"3M")
print(model2.predict())
#%%
print([e for e in stocksPrecios  if  e not  in stocksFundamental ])
stocks=set.intersection(set(stocksPrecios), set(stocksFundamental))
#%%
modelos={}
ob = obtenerModelos.ObtenerModelos()
stocksModels=[]
for stock in stocks:
    try:
        ex=stock.split("_")[0]
        st=stock.split("_")[1]
        data=bd.getDataByStock(tipo="fundamental", exchange=ex,stock=st,columnas=["netIncome"])
        nas,minimo,model,model2,fechaI=ob.getModelo(st,ex,"netIncome",data,"3M")
        print(model2.predict())
        stocksModels.append(stock)
    except Exception as e:
        pass