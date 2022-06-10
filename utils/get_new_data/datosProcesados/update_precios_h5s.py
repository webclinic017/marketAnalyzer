



import os
os.chdir("../../../")
from utils.database import bd_handler
bd = bd_handler.bd_handler("stocks")
import pandas as pd
import configparser
import time
import datetime as dt
from datetime import timedelta
from logging import getLogger
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')

if __name__=="__main__":
    config = configparser.ConfigParser()
    config.read('config/config_key.properties')
    directorio = config.get('ARCHIVOS', 'archivos_h5s_precios')
    sql = "show tables"
    tablas = bd.execute_query(sql)
    tablas = [tabla[0] for tabla in tablas]
    fechaI = (dt.datetime.today() - timedelta(days=65)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    TABLAS = [tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "precios")]
    for tabla in TABLAS:

        exchange = tabla.split("_")[0]
        if exchange != "US":
            existe = True
            try:
                filename = directorio + "precios_mensual_" + exchange
                h5s = pd.HDFStore(filename + ".h5s", "r")
                temp = h5s["data"]
                h5s.close()

            except Exception as e:
                print(e)

                existe = False


            try:
                tiempo1 = time.time()
                if existe:
                    data = bd.execute_query_dataframe(
                        "select fecha,stock,Adjusted_close from {}_precios  where fecha>%s order by fecha asc".format(
                            exchange), (fechaI,))
                else:
                    data = bd.execute_query_dataframe(
                        "select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange), None)

                data["fecha"] = pd.to_datetime(data["fecha"])
                data = data.groupby([data["stock"], data["fecha"].dt.year, data["fecha"].dt.month], as_index=False).last()
                data["fecha"] = data["fecha"].transform(lambda x: x.replace(day=1))
                data.set_index(["fecha", "stock"], inplace=True)
                data = data[~data.index.duplicated(keep='last')]


                logger.info("update_precios_h5s: {}: {} datos".format(exchange,len(data)))
                if existe:
                    temp = temp.loc[temp.index.get_level_values(0) <= fechaI]
                    logger.info("update_precios_h5s: {} ".format( temp.tail()))
                    logger.info("update_precios_h5s: {}".format(data.head()))
                    data = pd.concat([temp, data], axis=0)
                    data = data[~data.index.duplicated(keep='last')]
                    data.sort_index(inplace=True)



                filename = directorio + "precios_mensual_" + exchange

                h5s = pd.HDFStore(filename + ".h5s", "w")
                h5s["data"] = data
                h5s.close()
            except Exception as e:
                logger.error("update_precios_h5s: Error")


