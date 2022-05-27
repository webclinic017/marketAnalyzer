import datetime as dt
import os
#os.chdir("../")
from config import load_config
config = load_config.config()
from database import dabase_functions

import json

from database import  dabase_functions
def obtener_fechas_crecimiento_decrecimiento(serie,nombre,diccionarioFechas):
    diccionarioFechas[nombre]={"mucho_crecimiento":[],"crecimiento":[],
                                   "decrecimiento":[],"mucho_decrecimiento":[]}
    for e in serie.index:
        e = dt.datetime.strftime(e, "%Y/%m/%d")
        if serie.loc[e, nombre] < -0.15:
            diccionarioFechas[nombre]["mucho_decrecimiento"].append(e)
        elif serie.loc[e, nombre] < 0:
            diccionarioFechas[nombre]["decrecimiento"].append(e)
        elif serie.loc[e, nombre] < 0.15:
            diccionarioFechas[nombre]["crecimiento"].append(e)
        else:
            diccionarioFechas[nombre]["mucho_crecimiento"].append(e)



if __name__=="__main__":
    diccionarioFechas = {}
    indices = config["macro"]["indices"]
    macro_us = dabase_functions.get_multiple_macro_data("united states", shift=0)
    for indice in list(indices):
        diccionarioFechas[indice]={"mucho_crecimiento":[],"crecimiento":[],
                                   "decrecimiento":[],"mucho_decrecimiento":[]}

        country = config["macro"]["indice_country"][indice]
        exchanges = config["macro"]["country_exchange"][country]


        try:
            serie = dabase_functions.get_series_macro(config["macro"]["indice_country"][indice], indice, "index",
                                                      "2Q", indice)
            print(indice, country, serie.tail())
            serie = serie.pct_change(1).dropna()
            obtener_fechas_crecimiento_decrecimiento(serie, indice, diccionarioFechas)
        except Exception as e:
            print(e)


    with open("data/raw/dictionaries/fechas.json","w") as file:
            json.dump(diccionarioFechas,file)
