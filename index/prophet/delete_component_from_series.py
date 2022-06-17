# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os

print(os.getcwd())
os.chdir("../../")
import pandas as pd
from config import load_config
from functions import metricas
from functions.prophet import prophet_
from plots import plots_prophet
import datetime as dt
import numpy as np
from datetime import timedelta
from utils.database import database_functions,bd_handler
from functions.estacionaridad import estacionaridadYCointegracion
from prophet.plot import plot_cross_validation_metric
import matplotlib.pyplot as plt
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    config = load_config.config()
    pais="united states"
    feature="cpi (yoy)"
    variables_externas=[]
    bd_stocks=bd_handler.bd_handler("stocks")
    bd_market_data=bd_handler.bd_handler("market_data")
    data=database_functions.get_multiple_macro_data(pais,0)

    fecha_minima = dt.datetime.strptime(config["series_temporales"]["fecha_largo"], "%Y-%m-%d")
    fecha_sep = dt.datetime.strptime(config["series_temporales"]["fecha_sep"],
                                     "%Y-%m-%d")  # fecha de sepracion en train y test
    fecha_fin = dt.datetime.today()
    params_default = config["prophet"]["params_default"]

    data = data.loc[data.ds >= fecha_minima]
    data = data.dropna()
    data = data.loc[data.ds <= fecha_fin]
    df_train = data.loc[(data.ds <= fecha_sep)]
    df_test = data.loc[(data.ds > fecha_sep)]
    cor = data.corr()
    print(cor)

    cutoffs = pd.to_datetime(config["prophet"]["cutoffs"])
    # tamaño minimo de entrenamiento (usado para los cutoffs)
    min_tam_train = config["prophet"]["min_tam_train"]

    if config["prophet"]["use_cutoffs"]:
        cutoffs = [e for e in cutoffs if
                   e > df_train.index[min_tam_train] and e < df_train.index[-1] - timedelta(days=horizont_int)]
        print(cutoffs)

    # plot de la variable a predecir
    plots_prophet.plot_variable_to_predict(data, columnas=["y"], title=feature)

    # estacionaridad
    resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data["y"])

    print("Hurst: {}".format(str(resultadosEst["hurst"])))
    print("Lambda adf: {}".format(resultadosEst["lambda_adf"]))
    print("Test adf: {}".format(resultadosEst["results_adf"]))
    # creacion del modelo
    model, forecast, forecast_full, smape = prophet_.train_and_predict(df_train, df_test, **params_default)

    # Realizar predicciones en train
    future_train = df_train.drop("y", axis=1)
    forecast_train = model.predict(future_train)
    forecast_train["y"] = df_train.reset_index()["y"]
    print("METRICAS usando media {}".format(
        metricas.all_metricas(forecast_train.y, np.ones(len(forecast_train.y)) * np.mean(forecast_train.y))))
    print("METRICAS entrenamiento {}".format(metricas.all_metricas(forecast_train.y, forecast_train.yhat)))
    print("METRICAS tendencia entrenamiento {}".format(metricas.all_metricas(forecast_train.y, forecast_train.trend)))
    print("METRICAS estacioanalidad anual entrenamiento {}".format(
        metricas.all_metricas(forecast_train.y, forecast_train.yearly)))
    print("METRICAS terminos aditivos entrenamiento {}".format(
        metricas.all_metricas(forecast_train.y, forecast_train.additive_terms)))

    print("METRICAS sin usar estacionalidad anual en entrenamiento {}".format(
        metricas.all_metricas(forecast_train.y, forecast_train.yhat - forecast_train.yearly)))
    if len(df_train.columns) > 2:
        print("SMAPE terminos aditivos extra regressors entrenamiento {}".format(
            metricas.smape(forecast_train.extra_regressors_additive, forecast_train.additive_terms)))
    # graficos para ver los componentes y las prediccines en train
    model.plot_components(forecast_full)
    plots_prophet.plot_real_predicted_values(model, forecast_full, df_test, title='Forecast')

    # grafico de validation
    plots_prophet.plot_separated(forecast_full, "Forecast vs real", fecha_sep)

    # cros validation con cutoffs o horizon
    if config["prophet"]["cross_validation"]:
        if not config["prophet"]["use_cutoffs"]:

            df2, df_cv = prophet_.make_cross_validation(model, period=period, horizon=horizon)
        else:
            df2, df_cv = prophet_.make_cross_validation(model, period=period, horizon=horizon, cutoffs=cutoffs)
        indices = np.unique(df_cv.cutoff)

        # graficos y metricas para cada k-fold
        for i, ind in enumerate(indices):




            dat = df_cv.loc[df_cv.cutoff == ind]
            d1 = forecast_full.loc[forecast_full.ds < ind]
            model.plot_components(pd.concat([d1, dat]))
            plots_prophet.plot_separated(pd.concat([d1, dat]), feature, ind)
            metrics = metricas.all_metricas(dat.yhat, dat.y)
            if i < len(indices) - 1:
                print('METRICAS desde fecha {} hasta fecha: {},{}'.format(ind, indices[i + 1], metrics))
            else:
                print('METRICAS desde fecha {} hasta fecha: {}, {}'.format(ind, forecast_full.ds.values[-1], metrics))

    # metricas en cross validation y validation test

    fig = plot_cross_validation_metric(df_cv, metric='mape')
    fig = plot_cross_validation_metric(df_cv, metric='smape')
    fig = plot_cross_validation_metric(df_cv, metric='rmse')
    plt.show()
    print("METRICAS medio en cross validation: rmse={}, smape={}, mape={},".format(df2["rmse"].mean(),
                                                                                   df2["smape"].mean(),
                                                                                   df2["mape"].mean()))
    print("METRICAS en validation set: {}".format(metricas.all_metricas(forecast.y, forecast.yhat)))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
