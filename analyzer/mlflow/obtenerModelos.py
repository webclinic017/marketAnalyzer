#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 09:54:59 2022

@author: manuel
"""
import predicciones as pr
from mlflow.tracking import MlflowClient
import mlflow
import sys
import os
retval=os.getcwd()
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(os.getcwd())
sys.path.append("../functions")
import transformationsDataframes
import transformations
import pandas as pd
import math
import numpy as np
os.chdir(retval)
print(os.getcwd())
class ObtenerModelos():
    def __init__(self):
      
        mlflow.set_tracking_uri("http://localhost:5000")
        self.client=client = MlflowClient()
    def getModelo(self,stock,exchange,column,data1,periodoIndice):
        data=data1.copy()
        filter_string = "name ='{}'".format(exchange+"_"+stock+"_"+column+"_SARIMA")
        model=self.client.search_registered_models(filter_string=filter_string)
        if len(model)>0:
            model=model[0]
        else:
            return None
        array=model.name.split("_")
        caracteristicas=model.latest_versions[0]
        info=pr.InfoCaso(array[0],array[1],caracteristicas.run_id,caracteristicas.version)
        run=self.client.get_run(info.run_id)
        #print_run_info(run)
        model_name=info.exchange+"_"+info.stock+"_"+column+"_SARIMA"
        model_version=info.version
        retval=os.getcwd()
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        model = mlflow.statsmodels.load_model(
                    model_uri=f"models:/{model_name}/{model_version}"
                )
        os.chdir(retval)
        exponencial=True if run.data.params['transformExp']=="True" else "False"
     
        minimo=float(run.data.params['transformTrasl'])
        divisor=float(run.data.params['transformScale'])
        nas=float(run.data.params['numDatosNulos'])
        data=transformationsDataframes.pasarAMensual(data)
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
        print(model.specification)
        
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
        return nas,minimo,model,model.extend(serie_test1),fechaI