
import os
import numpy as np
import pandas as pd
from utils.get_new_data.EOD.getFundamentals import getFundamentals
from utils.get_new_data import db_functions
PRINT = False
pd.options.mode.chained_assignment = None
mycursorStocks, mydbStocks, data_dir,engine,  directorio_almacenamiento =db_functions.init('DatabaseStocksSection')
exchanges = pd.read_csv(data_dir + "exchanges.csv")
exchanges.set_index(["Name"], inplace=True, drop=True)




def UpdateDatosEnBD(precios, exchange, stock):
    an_array = np.full((len((precios))), exchange)
    precios["exchange"] = an_array
    an_array = np.full((len((precios))), stock)
    precios["stock"] = an_array
    precios.index.names = ["fecha"]
    precios.to_sql(exchange + "_fundamental", engine,
                   if_exists="append", chunksize=1000)

    mydbStocks.commit()


def comprobarSiExisteExchange(exchange):
    sql = "SHOW TABLES LIKE %s;"

    mycursorStocks.execute(sql, (exchange + "_fundamental",))
    codigo = mycursorStocks.fetchone()
    mydbStocks.commit()

    if codigo is None:
        precios = getFundamentals(
            "AMZN.US")
        u = precios
        u.drop(labels="currency_symbol", inplace=True, axis=1)
        cols = " double,".join([str(i) for i in u.columns.tolist()])
        cols = "fecha date," + cols
        cols = cols + \
               " double,stock varchar(100),exchange varchar(100), PRIMARY KEY (stock,exchange,fecha));"

        sql = "CREATE TABLE " + exchange + "_fundamental" + " (" + cols
        # print(sql)
        mydbStocks.cursor().execute(sql)
        mydbStocks.commit()
        # the connection is not autocommitted by default, so we must commit to save our # changes

    try:
        os.mkdir(directorio_almacenamiento + exchange)
    except Exception as e:
        e
