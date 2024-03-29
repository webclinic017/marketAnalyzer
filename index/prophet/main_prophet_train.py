# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
print(os.getcwd())
os.chdir("../../")
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzig_data')
import pandas as pd
from config import load_config
from functions import metricas
from functions.prophet import prophet_, logs_prophet
from plots import plots_prophet
import datetime as dt
from functions.prepare_data import dataframe_equities_with_options
import numpy as np
from datetime import timedelta
from functions.estacionaridad import estacionaridadYCointegracion
from prophet.plot import plot_cross_validation_metric
import matplotlib.pyplot as plt
from functions.estacionaridad import logs_estacionaridad
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 500)
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    config = load_config.config()

    data, feature, df_train, df_test ,fecha_sep = dataframe_equities_with_options.get_dataframe_default()

    # tamaño minimo de entrenamiento (usado para los cutoffs)
    min_tam_train = config["series_temporales"]["min_tam_train"]
    period = config["prophet"]["period"]
    cutoffs = pd.to_datetime(config["prophet"]["cutoffs"])
    horizon = config["prophet"]["horizon"]
    horizont_int = int(horizon.split(" ")[0])


    params_default = config["prophet"]["params_default"]



    if config["prophet"]["use_cutoffs"]:
        cutoffs = [e for e in cutoffs if
                   e > df_train.index[min_tam_train] and e < df_train.index[-1] - timedelta(days=horizont_int)]
        logger.info("main_prophet_train: {}".format(cutoffs))


    # plot de la variable a predecir
    plots_prophet.plot_variable_to_predict(data, columnas=["y"], title=feature)

    # estacionaridad
    resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data["y"])

    # logs of results
    logs_estacionaridad.log_stationarity_coef(resultadosEst)

    # creacion del modelo
    model, forecast, forecast_full = prophet_.train_and_predict(df_train, df_test, **params_default)

    # Realizar predicciones en train
    future_train = df_train.drop("y", axis=1)
    forecast_train = model.predict(future_train)
    forecast_train["y"] = df_train.reset_index()["y"]

    # logs of metrics
    logs_prophet.logs_metricas_prophet(forecast_train)

    # graficos para ver los componentes y las prediccines en train
    model.plot_components(forecast_full)
    plots_prophet.plot_real_predicted_values(model, forecast_full, df_test, title='Forecast')

    # grafico de validation
    plots_prophet.plot_separated(forecast_full, "Forecast vs real", fecha_sep)

    # cros validation con cutoffs o horizon
    if config["series_temporales"]["cross_validation"]:
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
