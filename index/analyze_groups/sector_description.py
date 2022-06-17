import os
os.chdir("../../")
from config import load_config
config = load_config.config()
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
import datetime as dt
from utils.database import bd_handler, database_functions
from utils.get_new_data import update_stock
from dateutil.relativedelta import relativedelta
from plots.plot_specific_series import plot_specific_series
import pandas as pd
if __name__=="__main__":
    bd=bd_handler.bd_handler("stocks")
    stocks_admiral=database_functions.filter_by_broker("admiralmarkets",bd=bd).accion.values.reshape(-1)
    stocks_key_word=database_functions.filter_by_description("%energy%", "%energies%", bd=bd).accion.values.reshape(-1)
    stocks=list(set(stocks_admiral).intersection(set(stocks_key_word)))
    stocks
    logger.info("sector_description: number of stocks to work {}".format(len(stocks)))




