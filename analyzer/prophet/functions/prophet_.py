from datetime import timedelta

import matplotlib.pyplot as plt
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_cross_validation_metric
from prophet.plot import add_changepoints_to_plot
from config import load_config
import itertools
from utils import info
config=load_config.config()
import random
from utils import metricas,plots
import pickle
import datetime as dt
def params_prophet(param_grid):

    all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
    n = config["prophet"]["number_combs"]
    all_params_2 = [e for e in all_params if e['changepoint_prior_scale'] != 0.05 or e['seasonality_prior_scale'] != 10]
    params_default = [e for e in all_params if
                      e['changepoint_prior_scale'] == 0.05 and e['seasonality_prior_scale'] == 10]
    indice = random.sample(range(0, len(all_params_2)), n)
    all_params_2 = [e for i, e in enumerate(all_params_2) if i in indice]
    len(params_default), len(all_params_2)
    return params_default,all_params_2

def train_prophet(params,fecha_inicio,fecha_sep,fecha_fin,df,cutoffs, horizon='60 days',variable_a_predecir=None):
    params1 = params.copy()

    for param in params1:
        cols = [e for e in df.columns if e in param.keys() and param[e] == True]
        columnas = [columna for columna in df.columns if columna in cols]
        columnas.append("y")
        columnas.append("ds")
        df_aux = df.loc[:, columnas]
        df_aux = df_aux.dropna()
        df_train = df_aux.loc[(df_aux.ds >=fecha_inicio)&(df_aux.ds <=fecha_sep)]
        df_test = df_aux.loc[(df_aux.ds > fecha_sep) & (df_aux.ds <= fecha_fin)]

        for e in df.columns:
            if e in param.keys():
                param.pop(e)

        model, forecast,forecast_full, smape = train_and_predict(df_train, df_test, **param)
        if config["prophet"]["cross_validation"]:

            df2,df_cv = make_cross_validation(model,cutoffs=cutoffs, horizon=horizon)
            print(f'SMAPE medio {df2["smape"].mean() } y RMSE medio {df2["rmse"].mean()} '
                  f'para params={param}, variables={cols}')

        print("SMAPE en validation {}".format(smape))
        #plot_forecast(forecast_full, "Forecast vs real")
        #plot_real_predicted_values(model, forecast_full, df_test)
        # Mostramos los componentes de Prophet
        #model.plot_components(forecast_full)
        if config["prophet"]["cross_validation"]:
            dict_save={"modelo":model,"metrics":df2,"params":param,"varoables":cols,"feature":variable_a_predecir,
                       "forecast":forecast,"df_train": df_train,"df_test":df_test, "cross_validation":df_cv,"smape_validation":smape}
        time = dt.datetime.now()
        with open("modelos/prophet_models/"+str(variable_a_predecir)+"_"+str(time), 'wb') as handle:
            pickle.dump(dict_save, handle, protocol=pickle.HIGHEST_PROTOCOL)




def separate_train_test(df, num_test_dates=10):
    if num_test_dates > df.shape[0]:
        num_test_dates = df.shape[0] / 2

    # Separamos en training y testing
    return df.loc[df.index[:-num_test_dates]], df.loc[df.index[-num_test_dates:]]


def train_and_predict(df_train, df_test, **params):
    monthly_seasonality = params["monthly_seasonality"]
    monthly_N = params["monthly_N"]
    weekly_N = params["weekly_N"]
    yearly_N = params["yearly_N"]
    use_holidays = params["use_holidays"]
    params.pop("monthly_seasonality")
    params.pop("monthly_N")
    params.pop("yearly_N")
    params.pop("weekly_N")
    params.pop("use_holidays")
    # Creamos un modelo prophet
    model = Prophet(weekly_seasonality=weekly_N, yearly_seasonality=yearly_N, **params)

    # Comprobar si se meten las variables externas
    columnas = list(set(df_train.columns) - set(("ds", "y")))
    for column in columnas:
        model.add_regressor(column)
    if use_holidays:
        model.add_country_holidays(country_name='US')

    if monthly_seasonality:
        model.add_seasonality(name='monthly', period=30.5, fourier_order=monthly_N)

    # Lo entrenamos con nuestro dataframe
    with info.suppress_stdout_stderr():
        model.fit(df_train)

    future = df_test.drop("y",axis=1)
    # Realizar predicciones



    forecast = model.predict(future)
    forecast["y"]=df_test.reset_index().y
    smape=metricas.SMAPE(forecast.y, forecast.yhat)
    aux=pd.concat([df_train,df_test],axis=0)
    future=aux.drop("y",axis=1)
    forecast1 =  model.predict(future)
    forecast1["y"] =aux.reset_index().y

    return model, forecast,forecast1,smape


def plot_forecast(forecast, title):
    figure = plt.figure(figsize=(15, 15))
    plt.plot(forecast.ds, forecast.y, marker="o", color="Blue")
    plt.plot(forecast.ds, forecast.yhat, marker="o", color="Orange")
    leyenda = ["Real", "Forecast"]
    plt.legend(leyenda)
    titulo = title
    plt.title(titulo)
    plt.show()


def make_cross_validation(model,cutoffs, horizon='60 days'):

    with info.suppress_stdout_stderr():
        df_cv = cross_validation(model,cutoffs=cutoffs, horizon=horizon)
        df_p = performance_metrics(df_cv)

        return df_p, df_cv


def plot_variable_to_predict(df, date_col='ds', columnas=None):


    for columna in columnas:
        if columna != "ds":
            plt.figure(facecolor='w', figsize=(8, 6))
            plt.plot(df[date_col], df[columna])
            plt.title(columna)
            plt.tight_layout()
            plt.show()


def modelo(params, variables, df, fecha):
    df_aux = df.loc[:, variables]
    df_aux = df_aux.dropna()
    df_train = df_aux.loc[df_aux.index <= fecha]
    df_test = df_aux.loc[df_aux.index > fecha]
    model, forecast = train_and_predict(df_train, df_test, future_periods=10, **params)

    return model, forecast, df_train, df_test


def plot_real_predicted_values(model, forecast, df_test, title='Forecast'):

    fig = model.plot(forecast)
    a = add_changepoints_to_plot(fig.gca(), model, forecast)
    datenow = df_test.iloc[0]['ds']

    plt.title(title, fontsize=12)
    plt.xlabel("Day", fontsize=12)
    plt.ylabel("Prize", fontsize=12)
    plt.axvline(datenow, color="k", linestyle=":")
    # Añadir puntos reales en rojo
    plt.scatter(df_test['ds'], df_test['y'], c='red')
    plt.tight_layout()
    plt.show()


