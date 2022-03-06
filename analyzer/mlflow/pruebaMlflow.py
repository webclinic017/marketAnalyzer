# The data set used in this example is from http://archive.ics.uci.edu/ml/datasets/Wine+Quality
# P. Cortez, A. Cerdeira, F. Almeida, T. Matos and J. Reis.
# Modeling wine preferences by data mining from physicochemical properties. In Decision Support Systems, Elsevier, 47(4):547-553, 2009.

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
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


if __name__ == "__main__":
    exchange="MC"
    indiceName="ibex35"
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
    precios=bd.getPrizesByExchange(exchange,columnas=columnasPrecios)
    fundamentals=bd.getFundamentalsByExchange(exchange,bd=True,columnas=columnas)
    indice=bd.getIndexPrizes(indiceName)
    if getsectors:
        sectors=bd.getSectors(exchange)
        fundamentals["sector"]=fundamentals["stock"].transform(lambda t:sectors[t])
    if   getdescriptions:
        descriptions=bd.getDescriptions(exchange)
        fundamentals["description"]=fundamentals["stock"].transform(lambda t:descriptions[t])
    if not_index:
        precios.reset_index(inplace=True)
        fundamentals.reset_index(inplace=True)
        
    events= bdMacro.devolverCalendario(exchange)
    import datetime as dt
    indice1=tfataframe.pasarAMensual(indice)
    macro=events.fillna(method="ffill").join(indice1.loc[:,["Close"]],how="left")
    macro=macro.loc[events.index>=dt.datetime(2012,3,1)].drop(["unemploymentchange","gdpyoy"],axis=1)
    macro
    warnings.filterwarnings("ignore")
    np.random.seed(40)
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import GridSearchCV,RandomizedSearchCV
    y=macro.loc[:,macro.columns[7]]>=macro.loc[:,macro.columns[7]].shift(1)
    X=macro.loc[:,macro.columns[:7]]
    X_train, X_test, y_train, y_test = train_test_split(
                                    X,
                                    y,
                                    train_size   = 0.7,
                                    random_state = 1234,
                                    shuffle      = False
                                )
    
    
  
  
    


    alpha = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    l1_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5

    with mlflow.start_run():
        gbrt = GradientBoostingClassifier(max_depth=3, n_estimators=10, learning_rate=0.1)
        #y_pred = cross_val_predict(gbrt , X, y, cv=5)
        #np.mean(y_pred==y)
        param_grid=[{"max_depth":np.arange(1,7,1),"n_estimators":np.arange(1,10,1),"learning_rate":np.arange(0.001,1,0.005)}]
        #grid_search = GridSearchCV(svc_3,param_grid, cv=5,
        #scoring="f1")
        grid_search = RandomizedSearchCV(gbrt ,param_grid, cv=3,
        scoring="accuracy")
        grid_search.fit(X_train,y_train)
        params=grid_search.best_params_
        gbrt = GradientBoostingClassifier(max_depth=params['max_depth'], n_estimators=params['n_estimators'], learning_rate=params['learning_rate'])
        gbrt.fit(X_train,y_train)
        #y_pred = cross_val_predict(gbrt , X, y, cv=3)
        y_pred=gbrt.predict(X_test)
        accuracy=np.mean(y_pred==y_test)
        

   

        mlflow.log_param('max_depth', params['max_depth'])
        mlflow.log_param('n_estimators', params['n_estimators'])
        mlflow.log_param('learning_rate', params['learning_rate'])
        mlflow.log_metric("accuracy", accuracy)
       

        tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme

        # Model registry does not work with file store
        if tracking_url_type_store != "file":

            # Register the model
            # There are other ways to use the Model Registry, which depends on the use case,
            # please refer to the doc for more information:
            # https://mlflow.org/docs/latest/model-registry.html#api-workflow
            mlflow.sklearn.log_model( gbrt, "model", registered_model_name="GradientBoosting")
        else:
            mlflow.sklearn.log_model(gbrt, "model")