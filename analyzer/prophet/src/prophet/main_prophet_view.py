


import os

import numpy as np

os.chdir("../../")
from functions import prophet_
import pickle
import pandas as pd
import matplotlib.pyplot as plt
from utils import  plots
from functions import prophet_
if __name__=="__main__":
    dir="modelos/prophet_models/"
    nombres_file=os.listdir(dir)
    diccionario_objetos={}
    dataframe=pd.read_csv("data/raw/dayly_energy.csv",index_col=0)

    for symbol in dataframe.columns:

        minimo=10
        for nombre in nombres_file:


            if len(nombre.split("_"))>1 and len(symbol.split("_"))>1 \
                and nombre.split("_")[1]==symbol.split("_")[1]:


                with open(dir+nombre,"rb") as file:
                    obj=pickle.load(file)
                    modelo=obj["modelo"]
                    print(symbol,obj["params"],obj["varoables"],obj["metrics"]["smape"].mean(),obj["smape_validation"])
                    if obj["metrics"]["smape"].mean()<minimo:
                        minimo=obj["metrics"]["smape"].mean()
                        diccionario_objetos[symbol]=obj
                        print(minimo)

                    """aux = pd.concat([obj["df_train"], obj["df_test"]], axis=0)
                    future = aux.drop("y", axis=1)
                    forecast1 = modelo.predict(future)
                    forecast1["y"] = aux["y"]
                    modelo.plot_components(forecast1)"""
                    plt.show()
        symbol

    for symbol,obj in diccionario_objetos.items():
            print(symbol)
            modelo = obj["modelo"]
            print("SMAPE cross-validation {}".format(obj["metrics"]["smape"].mean()))
            print("SMAPE validation {}".format(obj["smape_validation"]))
            print("Variables externas {}".format(obj["varoables"]))
            print("Parametros {}".format(obj["params"]))
            aux = pd.concat([obj["df_train"],obj["df_test"]], axis=0)
            future = aux.drop("y", axis=1)
            forecast1 = modelo.predict(future)
            forecast1["y"]=aux.reset_index()["y"]
            modelo.plot_components(forecast1)
            plt.title(symbol)
            plt.show()
            data=pd.DataFrame(obj["cross_validation"].values,columns=["date","yhat","low","high","y","fechaIni"])
            data["fechaIni"]=pd.to_datetime(data.fechaIni)
            data["date"] = pd.to_datetime(data.date)
            for fecha in np.unique((data.fechaIni)):
                aux=data.loc[data.fechaIni==fecha]
                plt.figure()
                t=forecast1.loc[forecast1.ds<=fecha].iloc[-600:]
                plt.plot(t.ds,t.y,c="green")
                plt.plot(t.ds, t.yhat,c="red")
                plt.plot(aux.date,aux.y,c="blue")
                plt.plot(aux.date, aux.yhat,c="orange")
                plt.title(symbol)
                plt.show()

            plt.figure()
            prophet_.plot_forecast(forecast1, "Forecast vs real")
            aux








