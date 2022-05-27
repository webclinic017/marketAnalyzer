import os

os.chdir("../../")
print(os.getcwd())
from utils import  plots
import datetime as dt
from config import load_config
import pandas as pd
if __name__ == '__main__':

    config = load_config.config()

    data=pd.read_csv("data/raw/dayly_energy.csv",index_col=0)
    data.index = pd.to_datetime(data.index)
    data["ds"] = data.index
    data["y"] = data[config["feature"]]

    fechas = [dt.datetime.strptime(e, "%Y-%m-%d") for e in config["fechas"].values()]
    fechas = [dt.datetime.strptime(e, "%Y-%m-%d") for e in config["fechas"].values()]
    for fecha in fechas:
        plots.plot_fechas(data, fecha)
