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
#%%
bd=bdStocks.getData()
from mlflow.tracking import MlflowClient
client = MlflowClient()
column="netIncome"
periodoIndice="3M"
import math
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
filter_string = "name LIKE '%{}%'".format(column)
models=client.search_registered_models(filter_string=filter_string)

for model in models:
    array=model.name.split("_")
    caracteristicas=model.latest_versions[0]
    print((caracteristicas))
    if int(caracteristicas.source.split("/")[2])==EXPERIMENT_ID:
        infoCaso=pr.InfoCaso(array[0],array[1],caracteristicas.run_id,caracteristicas.version)
    
        infos.append(infoCaso)

#%%
file=open("./resultados/resultados.txt","w")
for info in infos:
    
        print(info)
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
        #dividimos por 1M y eliminamos nas
        data=data.interpolate(method='linear')/divisor
        data=data.dropna()

        #pasamos a mensual y rellenamos el indice
        data=transformationsDataframes.pasarAMensual(data)
        all_days = pd.date_range(data.index[0], data.index[-1],freq=periodoIndice,normalize=True)
        all_days=all_days.map(lambda x:x.replace(day=1))
        ultimaFecha=data.index[-1]
        ultimoValor=data.iloc[-1] 
        data=data.reindex(all_days)
        if data.index[-1]!=ultimaFecha:
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
             else:
                  serie_test1=serie_test.copy()
                 
             boxcox=transformations.boxcox
             
             serie_test1=(serie_test1).applymap(lambda x:(boxcox(0,x)))
        else:
            serie_test1=serie_test.copy()
        
        segundo=model.extend(serie_test1).fittedvalues
        #obtenemos intervalos de predicciones
        dataframePred=model.get_forecast(len(serie_test)).summary_frame()
        dataframePred["real"]= serie_test1
        if exponencial:
            aux=minimo if minimo<0 else 0
            dataframePred=dataframePred.applymap(lambda x:(math.exp(x)+2*aux))
        if exponencial:
                 aux=minimo if minimo<0 else 0
                 primero=primero.map(lambda x:(math.exp(x)+2*aux))
                 segundo=segundo.map(lambda x:(math.exp(x)+2*aux))
        #hacemos el grafico con la parte de entrenamiento, la parte de prueba, valores ajustados en entrenamiento y valores ajustados en prueba
        dicc={"orders":model.specification["order"],"seasonal orders":model.specification['seasonal_order']}
        for param in model.params.index:
            dicc[param]=round(model.params.loc[param],2)
        print(model.specification)
        print(segundo.tail())
        
        graficos.plot_forecast(serie_train,dataframePred,pd.concat([primero,segundo]),title=info.exchange+"_"+info.stock+" ",extraInfo=dicc,fileName="plots/"+model_name)
file.close()
    
    
  
        
    
    