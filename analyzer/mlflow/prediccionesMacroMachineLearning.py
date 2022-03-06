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
    
                # There are other ways to use the Model Registry, which depends on the use case,
                # please refer to the doc for more information:
                # https://mlflow.org/docs/latest/model-registry.html#api-workflow
    import mlflow
    logged_model = 'runs:/a98f17f708d347f29dd0de7f78ab4bc5/gradientBoosting'
    
    # Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)
    
    # Predict on a Pandas DataFrame.
    import pandas as pd
    predicciones=loaded_model.predict(X_train)
    print(np.mean(predicciones==y_train))