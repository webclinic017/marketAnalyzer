import os
os.chdir("../../")
import configparser
import datetime as dt
import os
import sys
import warnings
from datetime import timedelta
import mysql.connector
import pandas as pd
import sqlalchemy
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
from utils.get_new_data import db_functions
class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout



from utils.get_new_data.EOD import getResults
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config/config.properties')
    section = 'DatabaseStocksSection'
    mycursor, mydb, data_dir, engine, directorio_almacenamiento = db_functions.init(section)
    DAYS_UPDATE_RESULTS = int(config.get('DatabaseStocksSection', 'days_update_results'))

    mycursorStocks = mydb.cursor()
    FECHA1 = "1990-01-01"
    FECHA2 = "2015-01-02"
    fecha1 = dt.datetime.strptime(FECHA1, "%Y-%m-%d")
    fecha2 = dt.datetime.strptime(FECHA2, "%Y-%m-%d")
    print("Fecha desde la que se actualizan resultados %s" % fecha1)
    print("Fecha hasta la que se actualizan resultados %s" % fecha2)
    sql = "delete from calendarioResultados where report_date>=%s and report_date<=%s"
    # mycursorStocks.execute(sql,(fecha1,fecha2))
    # mydb.commit()
    saveStock = True if config.get('DatabaseStocksSection', "saveStockFundamentalsWhenUpdatingResults") == "True" else False
    time_grid = pd.date_range(start=fecha1, end=fecha2, freq='10D').to_pydatetime()
    print("Numero de rangos de fechas %s" % (len(time_grid) - 1))
    for i in range(1, len(time_grid)):

        fecha1_aux = dt.datetime.strftime(time_grid[i - 1], "%Y-%m-%d")
        fecha2_aux = dt.datetime.strftime(time_grid[i] - timedelta(days=1), "%Y-%m-%d")
        print("Fecha inicial del rango %s %s" % (i, fecha1_aux))
        print("Fecha final del rango %s %s" % (i, fecha2_aux))
        resp = getResults.getResults(fecha1_aux, fecha2_aux)
        dataframe = pd.DataFrame(resp["earnings"])
        if len(dataframe) > 0:
            dataframe = dataframe.loc[:, [
                                             "code", "report_date", "date", "actual", "estimate"]]
            print(dataframe.tail(10))
            dataframe["stock"] = dataframe["code"].transform(lambda x: x.split(".")[0])
            dataframe["exchange"] = dataframe["code"].transform(
                lambda x: x.split(".")[1])
            dataframe.drop("code", inplace=True, axis=1)


            print("Número de resultados  en este rango %s" % len(dataframe))
            dataframe.to_sql("calendarioResultados", engine,
                             if_exists="append", chunksize=1000, index=False)
