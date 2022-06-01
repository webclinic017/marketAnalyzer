import os
os.chdir("../")
from database import dabase_functions
from config import load_config

from functions import macro
from plots import plots_iniciales
import pandas as pd
import json
import datetime as dt
import logging.config
logging.config.fileConfig("logs/logging.conf")
logger=logging.getLogger("errores")
def get_info_indice():
    pass




#se pretende analizar las correlaciones de los precios con diferentes variables
# se pretende analizar el comportamiento de los precios en periodos de crecimiento y decrecimiento
if __name__=="__main__":
    config = load_config.config()
    indices = config["macro"]["indices"]


    for indice in list(indices):
        country = config["macro"]["indice_country"][indice]
        exchanges = config["macro"]["country_exchange"][country]



        for exchange in exchanges:
            try:
                serie= dabase_functions.get_series_activos_diferentes_de_acciones(config["macro"]["indice_country"][indice], indice, "index", "2Q", indice)
                print(indice, country, exchange,serie.tail())
                serie=serie.pct_change(1).dropna()
                df_stocks_names=dabase_functions.filter_by_indice(indice,exchange)



                correlaciones_indice=[]
                correlaciones_us_macro=[]
                correlaciones_pais_propio_macro=[]
                correlaciones_funadmental=[]
                for stock in df_stocks_names.values:
                    print(stock)

                plots_iniciales.plot_serie_temporal(serie,indice,indice,4)

            except Exception as e:
                logger.error(e)











