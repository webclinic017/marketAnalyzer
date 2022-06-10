import os
os.chdir("../")

from config import load_config

config = load_config.config()
from plots import plots_iniciales
from utils.database import bd_handler,database_functions
if __name__ == "__main__":
    #analisis de los seleccionados para energias
    bd_market_data=bd_handler.bd_handler("market_data")
    data_macro=database_functions.get_multiple_macro_data("united states",0,bd_market_data)
    data_macro=data_macro.fillna(method="ffill")
    media=data_macro.mean()
    desv=data_macro.std()

    print(data_macro.tail())
    for column in data_macro.columns:
        plots_iniciales.plot_serie_temporal(data_macro.loc[:,[column]],kind=None,titulo=column,column=column)

