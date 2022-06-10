#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 16:42:32 2022

@author: manuel
"""
# archivo para guardar datos en formato h5s para leerlo directamente con pandas

import os
os.chdir("../../../")
from utils.database import bd_handler
bd = bd_handler.bd_handler("stocks")
import pandas as pd
import configparser
from logging import getLogger
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config/config_key.properties')
    directorio = config.get('ARCHIVOS', 'archivos_h5s_precios')

    sql = "show tables"
    tablas = bd.execute_query(sql)

    tablas = [tabla[0] for tabla in tablas]

    TABLAS = [tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "precios")]

    for tabla in TABLAS:
        exchange = tabla.split("_")[0]
        data = bd.execute_query_dataframe(
            "select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange), None)
        # data=engine.execute("select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange))
        data["fecha"] = pd.to_datetime(data["fecha"])
        data = data.groupby([data["stock"], data["fecha"].dt.year, data["fecha"].dt.month], as_index=False).last()
        data["fecha"] = data["fecha"].transform(lambda x: x.replace(day=1))
        data.set_index(["fecha", "stock"], inplace=True)
        data = data[~data.index.duplicated(keep='last')]
        filename = directorio + "precios_mensual_" + exchange
        h5s = pd.HDFStore(filename + ".h5s", "w")
        h5s["data"] = data
        try:
            h5s.close()
        except Exception as e:
            logger.error("guardar_precios_h5s: Error")
        filename = directorio + "precios_mensual_" + exchange
        h5s = pd.HDFStore(filename + ".h5s", "r")
        temp = h5s["data"]
        h5s.close()
        logger.info("guardar_precios_h5s:".format(temp.tail(10)))

