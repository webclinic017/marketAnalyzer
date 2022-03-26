#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 13:32:21 2022

@author: manuel
"""

import os
import warnings
import sys
import predicciones as pr
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

sys.path.append("../getData")
sys.path.append("../../visualization")
sys.path.append("../functions")
sys.path.append("../Models")
import bdStocks
import reports
import bdMacro
import analyseDataModels 
import graficosJupyterNotebook as graficos
import numpy as np
import pandas as pd
import transformationsDataframes as tfataframe
import configparser
config = configparser.ConfigParser()
config.read('../../config.properties')
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
EXPERIMENT_ID=2
ROUND=3
ROUND2=6
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
from datetime import timedelta
from multiprocessing import Process
config = configparser.ConfigParser()
config.read('../../config.properties')
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
mlflow.set_tracking_uri("http://localhost:5000")
import matplotlib
matplotlib.use('Agg')
from datetime import datetime
from dateutil.relativedelta import relativedelta
printPrecios=True
periodicidad=4
#%%
bd=bdStocks.getData()
from mlflow.tracking import MlflowClient
client = MlflowClient()
column="netIncome"
periodoIndice="3M"
import math
exchange="XETRA"
def print_run_info(r):
    
        print("run_id: {}".format(r.info.run_id))
        print("lifecycle_stage: {}".format(r.info.lifecycle_stage))
        print("metrics: {}".format(r.data.metrics))
        print("metrics: {}".format(r.data.params))
        # Exclude mlflow system tags
        tags = {k: v for k, v in r.data.tags.items() if not k.startswith("mlflow.")}
        print("tags: {}".format(tags))

#%%
infos=[]
filter_string = "name LIKE '"+exchange+"_%{}%'".format(column)
models=client.search_registered_models(filter_string=filter_string)

for model in models:
    array=model.name.split("_")
    caracteristicas=model.latest_versions[0]
    #print((caracteristicas))
    #print(array[1])
    if int(caracteristicas.source.split("/")[2])==EXPERIMENT_ID:
        infoCaso=pr.InfoCaso(array[0],array[1],caracteristicas.run_id,caracteristicas.version)
    
        infos.append(infoCaso)

#%%
sectors=bd.getSectors(exchange)
descriptions=bd.getDescriptions(exchange)
pd1=reports.PDF()
for info in infos:
    try:
        #print(info)
        run=client.get_run(info.run_id)
        #print_run_info(run)
        model_name=info.exchange+"_"+info.stock+"_"+column+"_SARIMA"
        model_version=info.version
     
        model = mlflow.statsmodels.load_model(
                    model_uri=f"models:/{model_name}/{model_version}"
                )
        exponencial=True if run.data.params['transformExp']=="True" else "False"
     
        minimo=float(run.data.params['transformTrasl'])
        divisor=float(run.data.params['transformScale'])
        nas=float(run.data.params['numDatosNulos'])
        #obtenemos los datos del stock
        data=bd.getDataByStock("fundamental",info.exchange,info.stock,bd=True,columnas=[column])
        print(info.stock)
        #dividimos por 1M y eliminamos nas
        data=data.interpolate(method='linear')/divisor
        data=data.dropna()

        #pasamos a mensual y rellenamos el indice
        data=transformationsDataframes.pasarAtrimestres(data)
        all_days = pd.date_range(data.index[0], data.index[-1],freq=periodoIndice,normalize=True)
        all_days=all_days.map(lambda x:x.replace(day=1))
        ultimaFecha=data.index[-1]
        penultimaFecha=data.index[-2]
        ultimoValor=data.iloc[-1] 
        data=data.reindex(all_days)
        data=data.interpolate(method='linear').dropna()
        if data.index[-1]!=ultimaFecha:
                    print(ultimaFecha)
                    data.loc[ultimaFecha]=ultimoValor
        #obtenemos la ultima fecha de train
        fechaI=model.predict().index[-1]
        #obtenemos train set y test set
        serie_test=data.loc[data.index>fechaI]
        serie_train=data.loc[data.index<=fechaI]
        #obtenemos predicciones y si es necesario transformamamos
        primero=model.fittedvalues
        #print(model.specification)
        
        
        if exponencial :
             if minimo<0:
                 serie_test1=serie_test-minimo*2
             elif minimo==0:
                  serie_test1=serie_test+10
             else:
                  serie_test1=serie_test.copy()
                
             boxcox=transformations.boxcox
             
             serie_test1=(serie_test1).applymap(lambda x:(boxcox(0,x)))
        else:
            serie_test1=serie_test.copy()
       
        segundo=model.extend(serie_test1).fittedvalues
        #obtenemos intervalos de predicciones
        """dataframePred=model.get_forecast(len(serie_test)).summary_frame()
        dataframePred["real"]= serie_test1
        if exponencial:
            aux=minimo if minimo<0 else 0
            dataframePred=dataframePred.applymap(lambda x:(math.exp(x)+2*aux))
        if exponencial:
                 aux=minimo if minimo<0 else 0
                 primero=primero.map(lambda x:(math.exp(x)+2*aux))
                 segundo=segundo.map(lambda x:(math.exp(x)+2*aux))
        
        #hacemos el grafico con la parte de entrenamiento, la parte de prueba, valores ajustados en entrenamiento y valores ajustados en prueba
        error=np.mean(abs(serie_test.values-segundo.values))
      
        print(model.predict())
        exit(-1)
        dicc={"orders":model.specification["order"],"seasonal orders":model.specification['seasonal_order'],"error":error/np.mean(abs(serie_test.values))}
        for param in model.params.index:
            dicc[param]=round(model.params.loc[param],2)
        #print(model.specification)
        #print(segundo.tail())
        graficos.plot_forecast(serie_train,dataframePred,pd.concat([primero,segundo]),title=info.exchange+"_"+info.stock+" ",extraInfo=dicc,fileName="plots/"+model_name)"""
        
        
        order=model.specification["order"]
        s_order=model.specification["seasonal_order"]
        inicioPred=max(order[0]+order[1],order[2]+order[1],s_order[0]*s_order[3]+s_order[1],s_order[2]*s_order[3]+s_order[1])
        
        pred=model.get_prediction(inicioPred,len(serie_train)-1).summary_frame()
        forecastOne=model.extend(serie_test1).forecast(1)
        dataframePred=model.get_forecast(len(serie_test)+1).summary_frame()
        dataframePred["fitted"]=segundo
        pred["fitted"]=model.fittedvalues
        dataframePred.loc[forecastOne.index[0],"fitted"]=forecastOne.iloc[0]
        
        if exponencial:
                     aux=minimo if minimo<0 else 0
                    
                     predicciones=segundo.map(lambda x:(math.exp(x)+2*aux))
                     forecastOne= forecastOne.map(lambda x:(math.exp(x)+2*aux))
                     dataframePred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]]=dataframePred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]].applymap(lambda x:(math.exp(x)+2*aux))
                     pred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]]=pred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]].applymap(lambda x:(math.exp(x)+2*aux))
                     dataframePred.loc[:,["mean_se"]]=dataframePred.loc[:,["mean_se"]].applymap(lambda x:(math.exp(x)))
                     pred.loc[:,["mean_se"]]=pred.loc[:,["mean_se"]].applymap(lambda x:(math.exp(x)))
        else:
            predicciones=segundo
        dataframePred["real"]=serie_test
        pred["real"]=serie_train
        pred=pd.concat([pred,dataframePred])
        print(pred.tail())
        graficos.plot_forecast_dataframe(pred,title=info.exchange+"_"+info.stock,extraInfo=None,fileName="plots/grafico"+model_name)
        if printPrecios:
              precios=bd.getDataByStock("precios",info.exchange,info.stock,bd=False,columnas=["Adjusted_close"])
              precios=transformationsDataframes.pasarAtrimestres(precios)
              pred["prize"]=precios
             
              pred=pred.loc[:,["real","fitted","prize"]]
              pred=(pred-pred.mean())/pred.std()
              
             
        error=np.mean(abs(serie_test.values-segundo.values))
        dicc={"orders":model.specification["order"],"seasonal orders":model.specification['seasonal_order'],"error":error/np.mean(abs(serie_test.values))}
        pred=pred.rename(lambda x: x+" "+column if x!="prize" else x,axis="columns")
        graficos.plot_forecast_dataframe(pred,title=info.exchange+"_"+info.stock+" "+"scaled",extraInfo=None,fileName="plots/"+model_name)
        
        des=None
        sec=None
        if info.stock in sectors.keys():
            sec=sectors[info.stock]
        if info.stock in descriptions.keys():
            des=descriptions[info.stock]
        texto="\n\n  Name : {}  sector : {}\n {}\n".format(info.exchange+"_"+info.stock,sec,des)
        title=info.exchange+": "+info.stock+" ("+sec+" )"
        texto+="Orders: {}\n".format(model.specification["order"])
        texto+=("Seasonal orders: {}\n".format(model.specification["seasonal_order"]))
        texto+=("Params: {}\n".format((dict(model.params.transform(lambda x:round(x,ROUND))))))
        texto+=("Test de heteroskedasticidad: {}\n".format(round(model.test_heteroskedasticity("breakvar",alternative= "two-sided")[0][1],ROUND)))
        texto+=("Test de normalidad: {}\n".format(round(model.test_normality(method="jarquebera")[0][1],ROUND)))
        u=(model.test_serial_correlation(method="ljungbox"))[0][1]
        texto+=("Test de colinealidad: {}\n".format([round(e,ROUND) for e in list(u)]))
        
        
      
        pd1.print_page(texto,"plots/"+model_name+".png","plots/grafico"+model_name+".png",title)
    except Exception as e:
         print(e)
pd1.output("../../reports/"+exchange+".pdf", 'F')
       
           
        
        
       
    
    
  
        
    
    