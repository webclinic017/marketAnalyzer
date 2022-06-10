
import numpy as np
import os
os.chdir("../../../")
from utils.database import bd_handler
bd = bd_handler.bd_handler("stocks")
import pandas as pd
import configparser
import warnings
import datetime as dt
from dateutil.relativedelta import relativedelta
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
from logging import getLogger
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
config = configparser.ConfigParser()
config.read('config/config_key.properties')
directorio = config.get('ARCHIVOS', 'archivos_h5s_precios')
insertar_nuevos_datos = True
insertar = True
fecha_delete = dt.date.today() - relativedelta(months=2)
fecha_delete = fecha_delete.replace(day=1)
logger.info("ratios_earnings: fecha_delete {}".format(fecha_delete))
sql = "show tables"
tablas = bd.execute_query(sql)
tablas = [tabla[0] for tabla in tablas]
tablas = [tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "fundamental")]

if __name__ == "__main__":
    for tabla in tablas:
        try:
            exchange = tabla.split("_")[0]


            exchangesIndex = {"BE": "DAX30", "NEO": "SPTSX", "LSE": "FTSE100", "MC": "IBEX35", "US": "SP500",
                              "XETRA": "DAX30", "VX": "SMI", "PA": "CAC40", "TO": "SPTSX", "MI": "FTSEMIB", "AS": "AEX"}
            filename = directorio + "precios_mensual_" + exchange
            h5s = pd.HDFStore(filename + ".h5s", "r")
            data = h5s["data"]
            h5s.close()
            COLUMNA_FUNDAMENTAL = ("select fecha,stock,commonStockSharesOutstanding,netIncome,totalAssets,totalLiab,totalcurrentLiabilities,netDebt,freeCashFlow,totalRevenue,ebit,ebitda,totalStockholderEquity\
                 from {}_fundamental order by fecha asc;".format(exchange), None)
            COLUMNA_RESULTADOS = (
            "select date,report_date,stock,exchange,actual as earnings from calendarioResultados where exchange=%s order by stock,report_date asc",
            (exchange,))

            data1 = bd.execute_query_dataframe(COLUMNA_FUNDAMENTAL[0], params=COLUMNA_FUNDAMENTAL[1])
            data1["fecha"] = pd.to_datetime(data1["fecha"])
            data1["fecha"] = data1.fecha.transform(lambda x: x.replace(day=1))
            data1.set_index(["fecha", "stock"], inplace=True, drop=False)
            data1 = data1[~data1.index.duplicated(keep='last')]
            datares =  bd.execute_query_dataframe(COLUMNA_RESULTADOS[0], params=COLUMNA_RESULTADOS[1])
            datares["date"] = pd.to_datetime(datares["date"])
            datares["report_date"] = pd.to_datetime(datares["report_date"])
            datares["date"] = datares.date.transform(lambda x: x.replace(day=1))
            datares.set_index(["date", "stock"], inplace=True, drop=False)
            datares = datares[~datares.index.duplicated(keep='last')]


            data2 = data1.groupby(data1.index.get_level_values(1)).commonStockSharesOutstanding.last()
            data1["commonStockSharesOutstanding"] = data1.apply(lambda x: data2[x["stock"]], axis=1)
            for c in data1.columns:
                if data1[c].dtypes == "float":
                    data1[c] = data1[c].groupby(level="stock").transform(lambda x: x.ewm(com=2).mean())
            data1["report_date"] = data1.apply(
                lambda x: datares.loc[(x.fecha, x.stock)].report_date if (x.fecha, x.stock) in \
                                                                         datares.index else None, axis=1)

            data1["report_date"] = data1.apply(
                lambda x: x.report_date if (not pd.isnull(x.report_date)) else x.fecha + relativedelta(months=3), axis=1)
            data1["fecha2"] = data1.report_date.map(
                lambda x: x.replace(year=(x.year + (x.month) // 12), month=(x.month + 1) % 12 if x.month != 11 else 12,
                                    day=1))

            data1["Adjusted_close"] = data1.apply(
                lambda x: data.loc[(x.fecha2, x.stock)].Adjusted_close if (x.fecha2, x.stock) in \
                                                                          data.index else np.nan, axis=1)

            data1 = data1.drop("fecha2", axis=1)

            data1["earnings"] = data1.apply(lambda x: datares.loc[(x.fecha, x.stock)].earnings if (x.fecha, x.stock) in \
                                                                                                  datares.index else np.nan,
                                            axis=1)
            data1["exchange"] = exchange
            data1 = data1.drop(["fecha", "stock"], axis=1)
            data1["per"] = data1.Adjusted_close * data1.commonStockSharesOutstanding / data1.netIncome
            data1["earnings_per"] = data1.Adjusted_close / data1.earnings
            data1["prizebook"] = data1.Adjusted_close * data1.commonStockSharesOutstanding / data1.totalAssets
            data1["solvenciaLargo"] = data1.totalStockholderEquity / data1.totalLiab
            data1["solvenciaCorto"] = data1.totalStockholderEquity / data1.totalcurrentLiabilities
            data1["liquidez"] = data1.freeCashFlow / data1.totalcurrentLiabilities

            data1 = data1.transform(lambda x: round(x, 5) if x.dtype == float else x)
            data1.replace([np.inf, -np.inf], np.nan, inplace=True)
            if insertar_nuevos_datos:
                try:
                    bd. execute("delete from ratios_results where exchange=%s and report_date>=%s", (exchange, fecha_delete))
                    indice = bd.execute_query_dataframe("select fecha,stock from ratios_results where exchange=%s",
                                                        params=(exchange,))
                    logger.info("ratios_earnings: delete since fecha_delete {}".format(fecha_delete))
                    indice["fecha"] = pd.to_datetime(indice["fecha"])
                    indice = pd.MultiIndex.from_frame(indice)
                    dat = data1.index.isin(indice)
                    data1 = data1[~dat]
                except Exception as e:
                    logger.error("ratios_earnings: No existen aun datos del exxchange {}".format(exchange))
            if insertar:

                bd.bulk_insert(data1,"ratios_results")
                logger.info("ratios_earnings: inserted {}".format(exchange))
        except OSError as e:
            logger.error("ratios_earnings: {} ".format(e))
        except TypeError as e:
            logger.error("ratios_earnings: {} ".format(e))
