import os
os.chdir("../")
from database import bd_handler


import pandas as pd
from config import load_config

config = load_config.config()
from functions import estacionaridadYCointegracion
import datetime as dt
from utils import work_dataframes
from plots import plots_iniciales
import pandas as pd
from plots import plots_iniciales
from functions import macro
if __name__ == "__main__":
    #analisis de los seleccionados para energias

    data_macro=macro.get_data("united states")
    data_macro=data_macro.fillna(method="ffill")
    media=data_macro.mean()
    desv=data_macro.std()

    print(data_macro.tail())
    for column in data_macro.columns:
        plots_iniciales.plot_serie_temporal(data_macro.loc[:,[column]],kind=None,titulo=column,column=column)

