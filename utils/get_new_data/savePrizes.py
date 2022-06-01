import os
os.chdir("../../")
import savePrizes_functions
import numpy as np
import pandas as pd
import json
from utils.get_new_data import db_functions
section='DatabaseStocksSection'
mycursorStocks, mydbStocks, data_dir, _, directorio_almacenamiento = db_functions.init(section)
if __name__ == "__main__":
    sql = "show tables"
    mycursorStocks.execute(sql)
    tablas = mycursorStocks.fetchall()
    mydbStocks.commit()

    sql = "select * from indice_exchange;"
    mycursorStocks.execute(sql)
    dicIndices = mycursorStocks.fetchall()
    mydbStocks.commit()

    dicIndices={e[1]:e[0] for e in dicIndices}
    tablas = [tabla[0] for tabla in tablas]
    # creamos las tablas a partir de las de precios
    TABLAS = [tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "precios")]

    print(TABLAS)
    kk = 0
    ex_loss = 0
    for tabla in TABLAS:

        print(tabla)
        exchange = tabla.split("_")[0]
        if exchange != "US":
            sql = "select code from degiro where exchange=%s and type='Common Stock'"
            mycursorStocks.execute(sql, (exchange,))
            companys1 = np.array(mycursorStocks.fetchall())
            sql = "select code from admiralmarkets where exchange=%s and type='Common Stock'"
            mycursorStocks.execute(sql, (exchange,))
            companys2 = np.array(mycursorStocks.fetchall())
        else:
            sql = "select code from admiralmarkets where exchange='NYSE' or exchange='NASDAQ' or exchange='NYSE ARCA' or exchange='NYSE MKT' and type='Common Stock'"
            mycursorStocks.execute(sql)
            companys1 = np.array(mycursorStocks.fetchall())
            companys2 = []
        mydbStocks.commit()
        companys=[]
        if exchange in dicIndices.keys():
            cadena = "%s,"*len(dicIndices[exchange])
            cadena = cadena[:-1]
            sql = "select company from indices where indice in ({})".format(cadena)
            mycursorStocks.execute(sql, tuple(dicIndices[exchange]))
            companys = np.array(mycursorStocks.fetchall())
            mydbStocks.commit()
        savePrizes_functions.comprobarSiExisteExchange(exchange)

        dataframe1 = pd.read_csv(data_dir+"exchanges/" +
                                 exchange+"/stocks.csv", dtype=str)
        dataframe1.set_index("Name", inplace=True, drop=True)

        for stock in dataframe1.Code:
            print(stock)
            if stock in companys or stock in companys1 or stock in companys2:
                savePrizes_functions.cargarPrecios(stock, exchange)
