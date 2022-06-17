

# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
os.chdir("../../")

print(os.getcwd())
import pandas as pd
from config import load_config
import datetime as dt
import random
from utils.dataframes import work_dataframes
from functions.estacionaridad import estacionaridadYCointegracion
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)
# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print(os.getcwd())
    random.seed(dt.datetime.now().minute)
    config = load_config.config()

    variables_externas =  ["U.S. 3M","U.S. 7Y", "U.S. 10Y", "U.S. 30Y"]

    data = pd.read_csv("data/raw/energies/dayly_energy.csv", index_col=0)  # datos concretos
    series=[]
    for column in data.columns:
       print(column)
       if len(column.split("_"))>1:
           series.append(
               dabase_functions.get_prize_or_fundamenal(column.split("_")[0], column.split("_")[1], "B", "precios", "adjusted_close", column))
       else:
           series.append(
               dabase_functions.get_series_activos_diferentes_de_acciones("oil", "Crude Oil WTI", "commodities", "D", "oil"))
    data= work_dataframes.merge(series)


    data.index = pd.to_datetime(data.index)
    estacionaridadYCointegracion.analizar_dataset(data)

    print(data.corr())



