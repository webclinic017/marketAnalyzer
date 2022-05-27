# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os



import pandas as pd
from config import load_config
from functions import prophet_

import datetime as dt
# Press the green button in the gutter to run the script.

def plot_fechas(data,fecha):

        config = load_config.config()

        data_plot = data.loc[data.ds > fecha]
        if "oil" in data_plot.columns:
                prophet_.plot_variable_to_predict(data_plot, columnas=["y", "oil"])






# See PyCharm help at https://www.jetbrains.com/help/pycharm/
