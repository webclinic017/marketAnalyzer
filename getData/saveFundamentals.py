#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 14:14:25 2021

@author: manuel
"""
import saveFundamental_functions
import numpy as np
import pandas as pd
import json

if __name__ == "__main__":
    mycursorStocks, mydbStocks, data_dir = saveFundamental_functions.init()
    archivo = open("../exchanges_index.json", "r")
    exchanges_index = json.load(archivo)
    exchanges = exchanges_index.keys()

    dicIndices = exchanges_index

    for exchange in exchanges:
        #exchange = dataframe.loc[exchange].Code

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
        cadena = "%s,"*len(dicIndices[exchange])
        mydbStocks.commit()

        cadena = cadena[:-1]
        # print(cadena)
        sql = "select company from indices where indice in ({})".format(cadena)
        mycursorStocks.execute(sql, tuple(dicIndices[exchange]))
        companys = np.array(mycursorStocks.fetchall())
        mydbStocks.commit()
        saveFundamental_functions.comprobarSiExisteExchange(exchange)
        dataframe1 = pd.read_csv(data_dir+"exchanges/" +
                                 exchange+"/stocks.csv", dtype=str)
        dataframe1.set_index("Name", inplace=True, drop=True)

        for stock in dataframe1.Code:

            if stock in companys or stock in companys1 or stock in companys2:
                
                saveFundamental_functions.cargarPrecios(stock, exchange)
