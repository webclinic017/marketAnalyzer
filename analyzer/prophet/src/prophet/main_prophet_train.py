# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
print(os.getcwd())
os.chdir("../../")
import pandas as pd
from config import  load_config
from functions import prophet_
from dateutil import relativedelta
import datetime as dt
import random


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(os.getcwd())
    random.seed(dt.datetime.now().minute)
    config=load_config.config()
    feature = config["feature"]



    data=pd.read_csv("data/raw/dayly_energy.csv", index_col=0)
    data1=data.copy()
    for feature in data1.columns:

        data=data1.copy()

        print(feature)

        variables_a_anadir = [e for e in data.columns if e != feature and e!="oil"]
        s=random.sample(variables_a_anadir,3)

        fecha_mimima = dt.datetime.strptime(config["fechas"]["fecha_largo"],"%Y-%m-%d")
        data.index=pd.to_datetime(data.index)
        data["ds"]=data.index
        data=data.loc[data.ds>fecha_mimima]
        data["y"]=data[feature]
        #prophet_.plot_variable_to_predict(data, columnas=["y","oil"])


        param_grid = config["prophet"]["param_grid"].copy()

        if    config["prophet"]["variables_externas"]:
            for e in s:
                print(e)
                param_grid[e] = [True, False]


        params_default,params_no_default=prophet_.params_prophet(param_grid)

        fecha_sep=dt.datetime.today()-relativedelta.relativedelta(months=6)
        fecha_fin=dt.datetime.today()
        cutoffs=["2017-01-01","2019-01-01","2020-05-01"]
        cutoffs=[dt.datetime.strptime(e,"%Y-%m-%d") for e in cutoffs]
        print(len(params_no_default),len(params_default))
        prophet_.train_prophet(params_default, fecha_mimima,fecha_sep,fecha_fin,data,cutoffs=cutoffs, horizon='400 days',variable_a_predecir=feature)
        prophet_.train_prophet(params_no_default, fecha_mimima, fecha_sep, fecha_fin, data, cutoffs=cutoffs, horizon='400 days',
                               variable_a_predecir=feature)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
