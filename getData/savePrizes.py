#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 14:14:25 2021

@author: manuel
"""
import savePrizes_functions
import numpy as np
import pandas as pd
import json
mycursorStocks,mydbStocks,data_dir=savePrizes_functions.init()
if __name__ == "__main__":
    archivo = open("../exchanges_index.json", "r")
    exchanges_index = json.load(archivo)
    exchanges = exchanges_index.keys()

    dicIndices = exchanges_index
    for exchange in exchanges:
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

            if stock in companys or stock in companys1 or stock in companys2:
                savePrizes_functions.cargarPrecios(stock, exchange)
