#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 13:32:21 2022

@author: manuel
"""

import os
import warnings
import sys
import matplotlib
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from urllib.parse import urlparse
import mlflow
import mlflow.sklearn
import logging
import sys
from datetime import timedelta
sys.path.append("../getData")
sys.path.append("../../visualization")
sys.path.append("../functions")
sys.path.append("../Models")
import SARIMA
import bdStocks
import bdMacro
import analyseDataModels 
import graficosJupyterNotebook as graficos
import numpy as np
import pandas as pd
import transformationsDataframes as tfataframe
import configparser
import transformations
import transformationsDataframes
import writer
from multiprocessing import Process
config = configparser.ConfigParser()
config.read('../../config.properties')
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
tam_train=float(config.get('EntrenamientoARIMA', 'tam_train'))
SCALE= True if config.get('Entrenamiento', 'scale')=="True" else False
def plot(data3,stock):
        fig=plt.figure(figsize=(13,13))
        plt.plot(data3.index,data3[column],label=column,marker="o")
        plt.plot(data3.index,data3[column2],marker="o",label=column2)
        plt.title(stock)
        plt.legend()
        plt.show(block=False)
def realizarAnalisis(serie_train,serie_test,serie):
    diferenciacion,diferenciacionEstacional=SARIMA.calcularDiferenciacion(serie_train, serie)
    estCorr,estPcorr=SARIMA.estadisticosCorrelaciones(serie_train,diferenciacion,diferenciacionEstacional)
    print( diferenciacion,diferenciacionEstacional)
    modelos=SARIMA.reglas(serie_train,serie_test,estCorr,estPcorr,diferenciacion,diferenciacionEstacional)
    modelos.sort(key=lambda t:t.aicTotal)
    return modelos
  
def analizarFundamental(fundamentals):
    
    indice=bd.getIndexPrizes(indiceName)
    if getsectors:
        sectors=bd.getSectors(exchange)
        fundamentals["sector"]=fundamentals["stock"].transform(lambda t:sectors[t])
    if   getdescriptions:
        descriptions=bd.getDescriptions(exchange)
        fundamentals["description"]=fundamentals["stock"].transform(lambda t:descriptions[t])
    #if not_index:
    #    precios.reset_index(inplace=True)
    #    fundamentals.reset_index(inplace=True)
    sectors=np.unique(fundamentals["sector"])
    dataframeAnalysis=fundamentals.loc[fundamentals["sector"].isin([sectors[-6]])]
    stocks=[e for e in np.unique(dataframeAnalysis["stock"]) if (dataframeAnalysis.loc[dataframeAnalysis["stock"]==e]).shape[0]>tamMinimo]
    print(stocks)
    stocks=iter(stocks)
    dataframeAnalysis.shape
    dicModelos={}
    for stock in stocks:
        #data1=dataframeAnalysis.loc[dataframeAnalysis["stock"]==stock,[column]]
        #data2=precios.loc[precios["stock"]==stock,["Adjusted_close"]]
        #data2= transformationsDataframes.pasarAMensual(data2)
        #data1=transformationsDataframes.pasarAMensual(data1)
        #data3=data1.join(data2).fillna(method="ffill")
        nas=0
        
        print("Stock %s"%stock)
        data=dataframeAnalysis.loc[dataframeAnalysis["stock"]==stock,[column]]
        nas+=int(data.isna().sum())
        
        data=data.fillna(method='ffill')/1000000
        nas-=int(data.isna().sum())
        data=data.dropna()
        #dataOriginal=data.copy()
        #data3=(data3-data3.mean())/data3.std()
        transformar=True
        if transformar:
            minimo=np.min(data)[0]
            if minimo<0:
                #print(minimo)
                data=data-minimo*2
            boxcox=transformations.boxcox
            data1=(data).applymap(lambda x:(boxcox(0,x)))
            #graficos.linearplot(data1,stock+" "+column,False,column)
            #graficos.linearplot(data1.transform(lambda x:x.pct_change()),stock+" "+column,False,column)
            #print(minimo)
        else:
            data1=data.copy()
        data1=transformationsDataframes.pasarAMensual(data1)
        all_days = pd.date_range(data1.index[0], data1.index[-1]+timedelta(days=diasDeMargen),freq=periodoIndice,normalize=True)
        all_days=all_days.map(lambda x:x.replace(day=1))
        nas+=len(all_days)-len(data1.index)
        data1=data1.reindex(all_days)
        serie=data1[column].fillna(method="ffill").dropna()
        tam=len(serie)
        lim_train=int(tam*tam_train)
        serie_train=serie[:lim_train]
        serie_test=serie[lim_train:]
        print(serie.shape,nas)
        #print(serie_test)
        modelos=realizarAnalisis(serie_train,serie_test,serie)
        dicModelos[stock]=modelos
    return dicModelos
        
#if __name__ == "__main__":
exchange="MC"
indiceName="ibex35"
getsectors=True
getdescriptions=False
columnas=["netIncome","totalRevenue","stock"]
columnasPrecios=["Adjusted_close","stock"]
tamMinimo=50
column="netIncome"
column2="Adjusted_close"
tam_train=0.8
periodicidad=4
nivel_confianza=0.1
max_lag=5
ponderaciones=[0.6,0.4]
transformar=False
periodoIndice="3M"
diasDeMargen=40
bd=bdStocks.getData()
#precios=bd.getPrizesByExchange(exchange,columnas=columnasPrecios)
fundamentals=bd.getFundamentalsByExchange(exchange,bd=True,columnas=columnas)
dicModelos=analizarFundamental(fundamentals)
#%%
for stock,modelos in dicModelos.items():
    for modelo in modelos:
        print(modelo.aicTotal)