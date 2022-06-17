import datetime as dt
import random

import pandas as pd

from config import load_config
from utils.database import database_functions, bd_handler
from utils.dataframes import work_dataframes
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzig_data')


def get_dataframe_default():
    bd_stocks = bd_handler.bd_handler("stocks")
    bd_market_data = bd_handler.bd_handler("market_data")
    random.seed(dt.datetime.now().minute)
    config = load_config.config()


    feature = "nasdaq"
    variables_externas = ["U.S. 3M", "U.S. 7Y", "U.S. 10Y", "U.S. 30Y"]
    data = database_functions.obtener_multiples_series("index", "D", *["ibex 35", "nasdaq", "s&p 500"],
                                                       bd=bd_market_data)
    data = database_functions.get_series_activos_diferentes_de_acciones("", "gold", "commodities", "D", "gold",
                                                                        bd=bd_market_data)

    array = []

    serie = database_functions.get_series_activos_diferentes_de_acciones("usa", "nasdaq", "index", "D", "nasdaq",
                                                                         bd=bd_market_data)
    array = [serie]
    for l in ["U.S. 3M", "U.S. 7Y", "U.S. 10Y", "U.S. 30Y"]:
        try:
            serie = database_functions.get_series_activos_diferentes_de_acciones("us", l, "bonds", "D", l,
                                                                                 bd=bd_market_data)
            array.append(serie)
        except Exception as e:
            print(e)
    data = work_dataframes.merge(array)

    data.index = pd.to_datetime(data.index)

    data["ds"] = data.index
    data["y"] = data[feature]

    freq = config["series_temporales"]["freq"]

    data = data.resample(freq).last()

    # macro
    pais = config["series_temporales"]["country_usado"]
    feature_macro = config["series_temporales"]["feature_macro"]
    data_macro = database_functions.get_multiple_macro_data(pais, shift=0, bd=bd_market_data)
    data_macro = data_macro.fillna(method="ffill")

    # si se usa macro para predecir
    variables_macro_externas = config["series_temporales"]["variables_macro_fijas"][pais]
    # si se quiere predecir una variable macro
    if feature_macro not in variables_macro_externas and config["series_temporales"]["predict_macro"]:
        variables_macro_externas.append(feature_macro)

    if config["series_temporales"]["usar_macro"]:
        array_merge = [data, data_macro.loc[:, variables_macro_externas]]
        data = work_dataframes.merge(array_merge, how="left")
        print(variables_macro_externas)

    # si se preice una variable macro
    if config["series_temporales"]["predict_macro"]:
        data["y"] = data.loc[:, feature_macro]
        data = data.drop(feature_macro, axis=1)
        variables_macro_externas.remove(feature_macro)
        feature = feature_macro

    columnas_dataframe = ["ds", "y"] + variables_externas
    if config["series_temporales"]["usar_macro"]:
        columnas_dataframe = columnas_dataframe + variables_macro_externas

    data = data.loc[:, columnas_dataframe]

    fecha_minima = dt.datetime.strptime(config["series_temporales"]["fecha_largo"], "%Y-%m-%d")
    fecha_sep = dt.datetime.strptime(config["series_temporales"]["fecha_sep"],
                                     "%Y-%m-%d")  # fecha de sepracion en train y test
    fecha_fin = dt.datetime.today()

    data = data.loc[data.ds >= fecha_minima]
    data = data.dropna()
    data = data.loc[data.ds <= fecha_fin]
    df_train = data.loc[(data.ds <= fecha_sep)]
    df_test = data.loc[(data.ds > fecha_sep)]
    cor = data.corr()
    logger.info("dataframe_equities_with_options: {}".format(cor))

    return data, feature, df_train, df_test, fecha_sep
