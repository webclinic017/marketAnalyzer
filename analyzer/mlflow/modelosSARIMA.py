#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 13:32:21 2022

@author: manuel
"""

import os
import warnings
import sys

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
experiment_id=3


def añadirModelo(modelo,stock,columna,experiment_id=3,**args):
    nombre=stock+"_"+columna+"_SARIMA"
    mlflow.set_tracking_uri("http://localhost:5000")

    with mlflow.start_run(run_name=nombre,experiment_id=experiment_id):
                    
        for param in modelo.modelo.params.index:
              
               
                mlflow.log_param(param, modelo.modelo.params.loc[param])
        for param,value in modelo.modelo.specification.items():
            
               
                mlflow.log_param(param, value)
        
        
        mlflow.log_metric("aicTest", modelo.aicTest)
        mlflow.log_metric("aicTrain", modelo.aicTrain)
        mlflow.log_metric("aicTotal", modelo.aicTotal)
        mlflow.log_param("ponderacion",args["ponderacion"])
        mlflow.log_param("transformExp",args["transformExp"])
        mlflow.log_param("transformTrasl",args["transformTrasl"])
        mlflow.log_param("numDatosTotales",args["numDatosTotales"])
        mlflow.log_param("numDatosNulos",args["numDatosNulos"])
        mlflow.log_param("transformScale",args["transformScale"])
        mlflow.log_metric("errorTrain",modelo.errorTrain)
        mlflow.log_metric("errorTest",modelo.errorTest)
        mlflow.log_metric("errorTotal",modelo.errorTotal)
       
     
        tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
    
        # Model registry does not work with file store
        if tracking_url_type_store != "file":
    
            # Register the model
            # There are other ways to use the Model Registry, which depends on the use case,
            # please refer to the doc for more information:
            # https://mlflow.org/docs/latest/model-registry.html#api-workflow
            mlflow.statsmodels.log_model(modelo.modelo, nombre, registered_model_name=nombre)
        else:
            mlflow.statsmodel.log_model(modelo.modelo, nombre,registered_model_name=nombre)
           