
import os
os.chdir("../..")
import configparser
import datetime as dt
import os
import sys
import warnings
from datetime import timedelta
import mysql.connector
import pandas as pd
import sqlalchemy
from utils.get_new_data import db_functions

class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout




from utils.get_new_data.EOD.getResults import getResults
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config/config.properties')
    warnings.filterwarnings("ignore")
    pd.options.mode.chained_assignment = None
    section='DatabaseStocksSection'
    mycursorStocks, mydb, data_dir, engine, directorio_almacenamiento = db_functions.init(section)
    saveStock = True if config.get('DatabaseStocksSection', "saveStockFundamentalsWhenUpdatingResults") == "True" else False

    fecha1 = dt.datetime.today() - timedelta(days=200)
    fecha2 = dt.date.today() + timedelta(days=30)

    time_grid = list(pd.date_range(start=fecha1, end=fecha2, freq='10D').to_pydatetime())
    if time_grid[-1] != fecha2:
        time_grid.append(fecha2)

    print("Fecha desde la que se actualizan resultados %s" % fecha1)
    sql = "delete from calendarioResultados where report_date>=%s"
    mycursorStocks.execute(sql, (fecha1,))
    mydb.commit()
    for i in range(1, len(time_grid)):
        if (time_grid[i - 1] != time_grid[i]):
            fecha1 = dt.datetime.strftime(time_grid[i - 1], "%Y-%m-%d")
            fecha2 = dt.datetime.strftime(time_grid[i] - timedelta(days=1), "%Y-%m-%d")
            print("Fecha inicial del rango %s %s" % (i, fecha1))
            print("Fecha final del rango %s %s" % (i, fecha2))
            resp = getResults(fecha1, fecha2)
            dataframe = pd.DataFrame(resp["earnings"]).loc[:, [
                                                                  "code", "report_date", "date", "actual", "estimate"]]
            dataframe["stock"] = dataframe["code"].transform(lambda x: x.split(".")[0])
            dataframe["exchange"] = dataframe["code"].transform(
                lambda x: x.split(".")[1])
            dataframe.drop("code", inplace=True, axis=1)
            # dataframe=dataframe[dataframe["actual"].notna()]

            print("Número de resultados  en este rango %s" % len(dataframe))
            dataframe.to_sql("calendarioResultados", engine,
                             if_exists="append", chunksize=1000, index=False)
