#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 17:17:19 2022

@author: manuel
"""
import pandas as pd
import sys
sys.path.append("../../analyzer/getData")
import bdStocks
bd= bdStocks.getData()
sectores=None
import datetime as dt
fechaI="2008-12-01"
fechaF="2022-06-01"
fechaI=dt.datetime.strptime(fechaI, '%Y-%m-%d')
fechaF=dt.datetime.strptime(fechaF, '%Y-%m-%d')
periodoIndice="3M"
if __name__=="__main__":
    print("Backtesting")
    
    datos=bd.executeQueryDataFrame("select fecha,stock,exchange,adjusted_close,per from ratios",None)
    datos["fecha"]=pd.to_datetime(datos["fecha"])
    datos.set_index(["fecha","exchange","stock"],inplace=True)