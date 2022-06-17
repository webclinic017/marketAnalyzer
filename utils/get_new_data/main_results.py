
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
class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout



fecha1 = dt.datetime.today() - timedelta(days=100)
fecha2 = dt.date.today() + timedelta(days=200)
from utils.get_new_data.stocks_data_EOD_implementation.get_results import get_results
if __name__ == "__main__":

    bd=bd_handler.bd_handler("stocks")
    time_grid = list(pd.date_range(start=fecha1, end=fecha2, freq='10D').to_pydatetime())
    if time_grid[-1] != fecha2:
        time_grid.append(fecha2)

    logger.info("main_results: Fecha desde la que se actualizan resultados %s" % (fecha1-timedelta(days=1)).date())
    sql = "delete from calendarioResultados where report_date>=%s"
    bd.execute(sql,(fecha1,))
    logger.info("main_results: Data deleted in calendarioResultados since date %s" % (fecha1-timedelta(days=1)).date())
    for i in range(1, len(time_grid)):
        if (time_grid[i - 1] != time_grid[i]):
            fecha1 = dt.datetime.strftime(time_grid[i - 1], "%Y-%m-%d")
            fecha2 = dt.datetime.strftime(time_grid[i] - timedelta(days=1), "%Y-%m-%d")
            logger.info("main_results: Fecha inicial del rango %s %s" % (i, fecha1))
            logger.info("main_results: Fecha final del rango %s %s" % (i, fecha2))
            resp = get_results(fecha1, fecha2)
            if len(resp["earnings"])>0:
                dataframe = pd.DataFrame(resp["earnings"]).loc[:, [
                                                                      "code", "report_date", "date", "actual", "estimate"]]
                dataframe["stock"] = dataframe["code"].transform(lambda x: x.split(".")[0])
                dataframe["exchange"] = dataframe["code"].transform(
                    lambda x: x.split(".")[1])
                dataframe.drop("code", inplace=True, axis=1)
                dataframe.set_index(["report_date"],drop=True,inplace=True)
                logger.info("main_results: Número de resultados  en este rango (fecha1={}, fecha2={}): {}".format(fecha1,fecha2,len(dataframe)))
                bd.bulk_insert(dataframe, "calendarioResultados")


