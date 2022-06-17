
import os
os.chdir("../../")

import datetime as dt
import os
import sys
import warnings
from datetime import timedelta
import pandas as pd
import logging, logging.config
from logging import getLogger
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
from utils.database import bd_handler
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
from utils.get_new_data.stocks_data_EOD_implementation.get_splits import get_splits
from utils.get_new_data.update_stock import   reinit_stock
class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout




from utils.get_new_data.stocks_data_EOD_implementation.get_splits import get_splits


if __name__ == "__main__":

    bd=bd_handler.bd_handler("stocks")
    fecha1 = dt.datetime.today() - timedelta(days=40)
    fecha2 = dt.date.today() - timedelta(days=3)
    fecha1 = dt.datetime.strftime(fecha1, "%Y-%m-%d")
    fecha2 = dt.datetime.strftime(fecha2, "%Y-%m-%d")

    logger.info("main_splits: Fecha inicial  %s" % ( fecha1))
    logger.info("main_splits: Fecha final %s " % ( fecha2))
    resp = get_splits(fecha1, fecha2)
    dataframe = pd.DataFrame(resp["splits"])
    dataframe["stock"] = dataframe["code"].transform(lambda x: x.split(".")[0])
    dataframe["exchange"] = dataframe["code"].transform(
        lambda x: x.split(".")[1])
    dataframe.drop("code", inplace=True, axis=1)
    #dataframe.set_index(["split_date"],drop=True,inplace=True)
    logger.info("main_splits: Número de resultados  (fecha1={}, fecha2={}): {}".format(fecha1,fecha2,len(dataframe)))

    for value in dataframe.iterrows():
            if value[1].exchange!='TSE':
                reinit_stock(value[1],bd)


