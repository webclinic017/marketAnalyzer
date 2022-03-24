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
import modelosSARIMA
from multiprocessing import Process
config = configparser.ConfigParser()
config.read('../../config.properties')
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
tam_train=float(config.get('EntrenamientoARIMA', 'tam_train'))
SCALE= True if config.get('Entrenamiento', 'scale')=="True" else False
DIVISOR=1000000
exchange="LSE"
indiceName="dax30"
columnas=["netIncome","totalRevenue","stock"]
columnasPrecios=["Adjusted_close","stock"]
tamMinimo=50
column="netIncome"
column2="Adjusted_close"
periodoIndice="3M"
experiment_id=2
periodicidad=4
comprobarEntrenado=True
from mlflow.tracking import MlflowClient
mlflow.set_tracking_uri("http://localhost:5000")
client = MlflowClient()
ponderaciones=[float(e) for e in (config.get('EntrenamientoARIMA', 'ponderaciones').split(" "))]
def plot(data3,stock):
        fig=plt.figure(figsize=(13,13))
        plt.plot(data3.index,data3[column],label=column,marker="o")
        plt.plot(data3.index,data3[column2],marker="o",label=column2)
        plt.title(stock)
        plt.legend()
        plt.show(block=False)
def realizarAnalisis(serie_train,serie_test,serie,stock,nas,minimo,transformar,DIVISOR):
    diferenciacion,diferenciacionEstacional=SARIMA.calcularDiferenciacion(serie_train, serie,periodicidad)
    estCorr,estPcorr=SARIMA.estadisticosCorrelaciones(serie_train,diferenciacion,diferenciacionEstacional,periodicidad)
    print( diferenciacion,diferenciacionEstacional)
    modelos=SARIMA.reglas(serie_train,serie_test,estCorr,estPcorr,diferenciacion,diferenciacionEstacional,periodicidad)
    modelos.sort(key=lambda t:t.aicTotal)
    if len(modelos)>0:
        modelo=modelos[0]
        dic={"ponderacion":ponderaciones[0],"numDatosTotales":len(serie),"numDatosNulos":nas,"transformTrasl":minimo,"transformExp":transformar,"transformScale":DIVISOR}
        modelosSARIMA.añadirModelo(modelo,exchange+"_"+stock,column,experiment_id=experiment_id,**dic)
    return modelos
  
def analizarFundamental(exchange):
    
    stocks=bd.getStocks(exchange)
    #stocks=[e for e in np.unique(dataframeAnalysis["stock"]) if (dataframeAnalysis.loc[dataframeAnalysis["stock"]==e]).shape[0]>tamMinimo]
    
    
    dicModelos={}
    for stock in stocks:
        if comprobarEntrenado:
            cad=exchange+"_"+stock+"_"+column+"_SARIMA"
            filter_string = "name='{}'".format(cad)
            models=client.search_registered_models(filter_string=filter_string)
            if(len(models))>0:
                print("Entrenado")
                continue
        try:
            #data1=dataframeAnalysis.loc[dataframeAnalysis["stock"]==stock,[column]]
            #data2=precios.loc[precios["stock"]==stock,["Adjusted_close"]]
            #data2= transformationsDataframes.pasarAMensual(data2)
            #data1=transformationsDataframes.pasarAMensual(data1)
            #data3=data1.join(data2).fillna(method="ffill")
            nas=0
            
            print("Stock %s"%stock)
            data=bd.getDataByStock("fundamental",exchange,stock,bd=True,columnas=[column])
            if len(data)>tamMinimo:
                nas+=int(data.isna().sum())
                
                data=data.interpolate(method='linear')/DIVISOR
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
                    elif minimo==0:
                        data=data+10
                    boxcox=transformations.boxcox
                    data1=(data).applymap(lambda x:(boxcox(0,x)))
                    #graficos.linearplot(data1,stock+" "+column,False,column)
                    #graficos.linearplot(data1.transform(lambda x:x.pct_change()),stock+" "+column,False,column)
                    #print(minimo)
                else:
                    data1=data.copy()
               
                data1=transformationsDataframes.pasarAMensual(data1)
                all_days = pd.date_range(data1.index[0], data1.index[-1],freq=periodoIndice,normalize=True)
                all_days=all_days.map(lambda x:x.replace(day=1))
                nas+=len(all_days)-len(data1.index)
                
                ultimaFecha=data1.index[-1]
                ultimoValor=data1.iloc[-1] 
                data1=data1.reindex(all_days)
                if data1.index[-1]!=ultimaFecha:
                    data1.loc[ultimaFecha]=ultimoValor
                    nas+=1
                serie=data1[column].interpolate(method='linear').dropna()
                tam=len(serie)
                lim_train=int(tam*tam_train)
                serie_train=serie[:lim_train]
                serie_test=serie[lim_train:]
                print(serie.shape,nas)
                print(data1.tail())
                #print(serie_test)
                modelos=realizarAnalisis(serie_train,serie_test,serie,stock,nas,minimo,transformar,DIVISOR)
                #dicModelos[stock]=modelos
      
        except Exception as e:
            continue
    return dicModelos
        


bd=bdStocks.getData()
#precios=bd.getPrizesByExchange(exchange,columnas=columnasPrecios)
dicModelos=analizarFundamental(exchange)
#%%
for stock,modelos in dicModelos.items():
    for modelo in modelos:
        print(modelo)
        print(modelo.aicTotal)
        u=modelo.modelo
        print(u.specification)
        print(u.params)
        for e in u.params.index:
            print(e,u.params.loc[e])