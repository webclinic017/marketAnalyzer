import os
os.chdir("../../")
import configparser
import datetime as dt
from datetime import timedelta
import investpy as invest
import mysql.connector
import pandas as pd
import sqlalchemy
from utils.get_new_data import db_functions
if __name__ == "__main__":
    mycursor, mydb, data_dir,engine,directorio_almacenamiento = db_functions.init('DatabaseStocksSection')
    mycursor2, mydb2, data_dir, engine2, directorio_almacenamiento = db_functions.init('DatabaseStocksSection','database_name2')

    countries = invest.indices.get_index_countries()
    us = invest.indices.get_indices(country="united states")
    spain = invest.indices.get_indices(country="spain")
    canada = invest.indices.get_indices(country="canada")
    france = invest.indices.get_indices(country="france")
    uk = invest.indices.get_indices(country="united kingdom")
    germany = invest.indices.get_indices(country="germany")
    italy = invest.indices.get_indices(country="italy")
    netherlands = invest.indices.get_indices(country="netherlands")
    sweden = invest.indices.get_indices(country="sweden")
    russia = invest.indices.get_indices(country="russia")
    brazil = invest.indices.get_indices(country="brazil")
    finland = invest.indices.get_indices(country="finland")
    china = invest.indices.get_indices(country="china")
    switzerland = invest.indices.get_indices(country="switzerland")
    japon = invest.indices.get_indices(country="japan")

    array = {"S&P 500": "united states", "FTSE 100": "united kingdom", "IBEX 35": "spain", "Nasdaq": "united states",
             "CAC 40": "france", \
             "DAX": "germany", "FTSE MIB": "italy", "AEX": "netherlands", "SMI": "switzerland", "Nikkei 225": "japan",
             "CSI 300": "china", "S&P/TSX 60": "canada"}
    array1 = ["sp500", "ftse100", "ibex35", "nasdaq", "cac40", "dax30", "ftsemib", "AEX", "SMI", "nickey",
              "shanghaiShenzhenCSI", "sptsx"]
    fecha2 = (dt.datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
    for nombreInvesting, nombreDB in zip(array.keys(), array1):
        sql = "select Date from  {} where indice=%s order by Date  desc limit 1".format("preciosIndices")
        mycursor.execute(sql, (nombreDB,))

        fecha = pd.DataFrame(mycursor.fetchone()).iloc[0, 0]
        fecha = dt.datetime(fecha.year, fecha.month, fecha.day)
        print("Fecha inicial para indice %s: %s" % (nombreDB, fecha))
        mydb.commit()
        data = invest.indices.get_index_historical_data(nombreInvesting, array[nombreInvesting],
                                                        from_date=fecha.strftime("%d/%m/%Y"), to_date=fecha2)
        data["indice"] = nombreDB
        data.index = pd.to_datetime(data.index, format="%d/%m/%Y")
        data = data.loc[data.index > fecha]
        data.to_sql("preciosIndices", engine, if_exists="append", chunksize=1000)
        sql = "select Date from  {} where indice=%s order by Date  desc limit 1".format("preciosIndices")
        mycursor2.execute(sql, (nombreDB,))
        fecha = pd.DataFrame(mycursor2.fetchone()).iloc[0, 0]
        fecha = dt.datetime(fecha.year, fecha.month, fecha.day)
        mydb.commit()
        data = invest.indices.get_index_historical_data(nombreInvesting, array[nombreInvesting],
                                                        from_date=fecha.strftime("%d/%m/%Y"), to_date=fecha2)
        data["indice"] = nombreDB
        data.index = pd.to_datetime(data.index, format="%d/%m/%Y")
        data = data.loc[data.index > fecha]
        data.to_sql("preciosIndices", engine2, if_exists="append", chunksize=1000)
