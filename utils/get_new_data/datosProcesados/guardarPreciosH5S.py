#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 16:42:32 2022

@author: manuel
"""
# archivo para guardar datos en formato h5s para leerlo directamente con pandas

import os
os.chdir("../../../")

from database import bd_handler
bd = bd_handler.bd_handler()
import pandas as pd
import configparser

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config/config.properties')
    directorio = config.get('ARCHIVOS', 'archivos_h5s_precios')

    sql = "show tables"
    tablas = bd.execute_consult(sql)

    tablas = [tabla[0] for tabla in tablas]

    TABLAS = [tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "precios")]

    for tabla in TABLAS:
        exchange = tabla.split("_")[0]
        data = bd.execute_consult_dataframe(
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
            pass
        filename = directorio + "precios_mensual_" + exchange
        h5s = pd.HDFStore(filename + ".h5s", "r")
        temp = h5s["data"]
        h5s.close()

        print(temp.tail(10))
