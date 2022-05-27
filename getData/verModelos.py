#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 21:06:12 2022

@author: manuel
"""
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
import sys
sys.path.append("../analyzer/mlflow")
sys.path.append("../analyzer/getData")
import obtenerModelos
import bdStocks
import entrenarModelo
bd= bdStocks.getData()
ob= obtenerModelos.ObtenerModelos()
column="netIncome"
column2="Adjusted_close"
if __name__ == "__main__":
    mycursorStocks, mydbStocks, data_dir = saveFundamental_functions.init()
    archivo = open("../exchanges_index.json", "r")
    exchanges_index = json.load(archivo)
    exchanges = exchanges_index.keys()

    dicIndices = exchanges_index

    for exchange in ["MC"]:
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
            #print(exchange+"_"+stock)
            print(exchange,stock)
            if stock in companys or stock in companys1 or stock in companys2:
               try: 
                minimo,modelo2,fechaI=ob.getModeloReduced(stock,exchange,"netIncome","3M")
                data=bd.getDataByStock("fundamental", exchange, stock,columnas=[column])
                minimo2=min(data[column])
                if minimo2<minimo:
                    print(stock)
                    entrenarModelo.analizarFundamental(exchange,stock)
                
                #minimo,modelo1,fechaI=ob.getModeloReduced(stock,exchange,"Adjusted_close","1M")
                #print(fechaI)
               except Exception as e:
                 pass
