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

SCALE= True if config.get('Entrenamiento', 'scale')=="True" else False

if __name__ == "__main__":
    exchange="US"
    indiceName="sp500"
    getsectors=True
    getdescriptions=False
    columnas=["netIncome","totalRevenue","stock"]
    columnasPrecios=["Adjusted_close","stock"]
    not_index=True
    tamMinimo=50
    mod=4
    nivel_confianza=0.05
    numberPreds=10
    bd=bdStocks.getData()
   
    indice=bd.getIndexPrizes(indiceName)

        
    events= bdMacro.devolverCalendario(exchange)
    import datetime as dt
    indice1=tfataframe.pasarAMensual(indice)
    macro=events.fillna(method="ffill").join(indice1.loc[:,["Close"]],how="left")
    if exchange=="MC":
        macro=macro.loc[events.index>=dt.datetime(2012,3,1)].drop(["unemploymentchange","gdpyoy"],axis=1)
    elif exchange=="US":
        macro=macro.loc[events.index>=dt.datetime(2008,2,1)].drop(["auction","bloombergconsumerconfidencey","gdpqoq","buildingpermitsmom"],axis=1)
    elif exchange=="XETRA":
        macro=macro.loc[events.index>=dt.datetime(2008,3,1)]
    elif exchange=="PA":
        macro=macro.loc[events.index>=dt.datetime(2008,8,1)]
    elif exchange=="LSE":
           macro=macro.loc[events.index>=dt.datetime(2008,3,1)].drop(["gdpyoy"],axis=1)   
    warnings.filterwarnings("ignore")
    np.random.seed(40)
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import GridSearchCV,RandomizedSearchCV
    m=len(macro.columns)-1
    y=macro.loc[:,macro.columns[m]]>=macro.loc[:,macro.columns[m]].shift(1)
    X=macro.loc[:,macro.columns[:m]]
    X_train, X_test, y_train, y_test = train_test_split(
                                    X,
                                    y,
                                    train_size   = 0.7,
                                    random_state = 1234,
                                    shuffle      = False
                                )
    modelos=["gradientBoosting","adaBoosting","randomForest","decisionTree","svm","logisticRegresion"]
    mlflow.set_tracking_uri("http://localhost:5000")

    for modelo in modelos:
        nombre=exchange+"_"+indiceName+"_"+modelo+"_"#+dt.datetime.now().strftime("%m/%d/%Y, %H:%M")

        with mlflow.start_run(run_name=nombre,experiment_id=1):
            if modelo=="gradientBoosting":
                gbrt,p,r,f1,ac,params=analyseDataModels.gradientBoostingClassify(X_train,y_train,X_test,y_test)
            
            elif modelo=="adaBoosting":
                gbrt,p,r,f1,ac,params=analyseDataModels.AdaBoostingClassify(X_train,y_train,X_test,y_test)
            elif modelo=="randomForest":                                                        
                gbrt,p,r,f1,ac,params=analyseDataModels.RandomForestClassify(X_train,y_train,X_test,y_test)
            elif modelo=="decisionTree":                                                            
                gbrt,p,r,f1,ac,params=analyseDataModels.DecissionTreeClassify(X_train,y_train,X_test,y_test)
            elif modelo=="svm":                                                                
                gbrt,p,r,f1,ac,params=analyseDataModels.LinearSVMClassify(X_train,y_train,X_test,y_test)
            elif modelo=="logisticRegresion":  
                gbrt,p,r,f1,ac,params=analyseDataModels.LogisticRegressionClassify(X_train,y_train,X_test,y_test)
            
            metrics={"precission":p,"recall":r,"f_score":f1,"accuracy":ac}
                    
            for param,value in params.items():
                
                   
                    mlflow.log_param(param, value)
                    
            for i,metric in metrics.items():
               
                if isinstance(metric, float):
                    mlflow.log_param(i, metric)
                else:
                    # 1 because in models first value is False class and second is True class
                    mlflow.log_metric(i, metric[1])
           
         
            tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
    
            # Model registry does not work with file store
            if tracking_url_type_store != "file":
    
                # Register the model
                # There are other ways to use the Model Registry, which depends on the use case,
                # please refer to the doc for more information:
                # https://mlflow.org/docs/latest/model-registry.html#api-workflow
                mlflow.sklearn.log_model( gbrt, nombre, registered_model_name=nombre)
            else:
                mlflow.sklearn.log_model(gbrt, nombre,registered_model_name=nombre)