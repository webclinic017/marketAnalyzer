import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics

from config import load_config
import itertools
from logs import info

config=load_config.config()
import random
from functions import metricas


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






def train_and_predict(df_train, df_test, **params):
    monthly_seasonality = params["monthly_seasonality"]
    monthly_n = params["monthly_N"]
    weekly_n = params["weekly_N"]
    yearly_n = params["yearly_N"]
    use_holidays = params["use_holidays"]

    for key in ["monthly_seasonality","monthly_N","yearly_N","weekly_N","use_holidays"]:
        if key in params.keys():
            params.pop(key)
    # Creamos un modelo prophet
    model = Prophet(weekly_seasonality=weekly_n, yearly_seasonality=yearly_n, **params)

    # Comprobar si se meten las variables externas
    columnas = list(set(df_train.columns) - set(("ds", "y")))
    for column in columnas:
        model.add_regressor(column)

    if use_holidays:
        model.add_country_holidays(country_name='US')

    if monthly_seasonality:
        model.add_seasonality(name='monthly', period=30.5, fourier_order=monthly_n)

    # Lo entrenamos con nuestro dataframe
    with info.suppress_stdout_stderr():
        model.fit(df_train)

    future = df_test.drop("y",axis=1)
    # Realizar predicciones



    forecast_test = model.predict(future)
    forecast_test["y"]=df_test.reset_index().y
    aux=pd.concat([df_train,df_test],axis=0)
    future=aux.drop("y",axis=1)
    forecast_full_dataframe =  model.predict(future)
    forecast_full_dataframe["y"] =aux.reset_index().y
    return model, forecast_test,forecast_full_dataframe




def make_cross_validation(model,period="60 days", horizon='60 days',cutoffs=None):
    with info.suppress_stdout_stderr():
        print(cutoffs)
        if cutoffs is None:
            print("Period {}, horizon {}".format(period,horizon))
            df_cv = cross_validation(model,period=period, horizon=horizon)
        else:
            df_cv = cross_validation(model, cutoffs=cutoffs, horizon=horizon)
        df_p = performance_metrics(df_cv)

        return df_p, df_cv




