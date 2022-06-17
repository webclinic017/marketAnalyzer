import os
from sympy.physics.units import years
os.chdir("../")
from config import load_config
config=load_config.config()
import logging, logging.config
from logging import getLogger
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
import datetime as dt
from utils.database import bd_handler, database_functions
from utils.get_new_data import update_stock
from dateutil.relativedelta import relativedelta
from plots.plot_specific_series import  plot_specific_series
from plots import other_plots

other_plots
import pandas as pd
import numpy as np
from statsmodels.stats.diagnostic import lilliefors
import warnings
warnings.filterwarnings("ignore")
logging.getLogger('matplotlib').setLevel(logging.ERROR)
check_normality=False
if __name__=="__main__":
    bd=bd_handler.bd_handler("stocks")
    fecha_ini=dt.datetime(2017,3,1)
    fecha_end = dt.datetime(2022, 1, 1)
    stocks=["US_GOOG","US_MSFT","US_QCOM","US_TSLA","US_NFLX","US_FB","US_PYPL","US_NVDA","US_ZM","US_RPD","US_SPLK","US_BIDU","US_PDD"]
    data = database_functions.obtener_multiples_series("precios",



    "B", *stocks, bd=bd)
    data=data.loc[(data.index>fecha_ini)&(data.index<fecha_end)]
    log_returns =np.log(data)
    log_returns=log_returns.diff()
    data=data.pct_change()
    covariance = data.cov()
    covariance.index = data.columns
    covariance.columns = data.columns
    cov_inv=pd.DataFrame(np.linalg.inv(    covariance.values))
    cov_inv.index = data.columns
    cov_inv.columns = data.columns
    medias=data.mean()
    weights=np.dot(cov_inv,medias)
    weights = {i: e for i, e in zip(data.columns, np.dot(cov_inv, medias))}
    logger.info("optimize_porfolio: wights:{}".format(weights))
    if check_normality:
        for column in log_returns.columns:
            serie=log_returns[column].dropna()
            serie=(serie-serie.mean())/(serie.std())
            test=lilliefors(serie)
            logger.info("optimize_porfolio: lilliefors test {}: {}".format(column,test))
            other_plots.distribucion_univariante(serie, column)


