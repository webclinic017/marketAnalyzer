#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 16:39:09 2022

@author: manuel
"""
from eod import  EodHistoricalData
import configparser
import pandas as pd
import sys
import os
os.chdir("EOD")
sys.path.append("../../analyzer/getData")
import bdStocks
pd.options.mode.chained_assignment = None
import pandas as pd
from functools import reduce
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
import datetime as dt
bd=bdStocks.getData()

import getHighLights
    


if __name__=="__main__":
    palabra_clave="%energy%"
    palabra_clave2="%cybersecurity%"
    consulta1="select stock,exchange from descriptions where description like %s "
    dataframe=bd.executeQueryDataFrame(consulta1,(palabra_clave,))
    """consulta2=("select company as stock,exchange from indices inner join exchangeIndice on exchangeIndice.indice=indices.indice;",None)
    consultas=[consulta1,consulta2]
    for consulta in consultas[1:]:
        dataframe=pd.concat([dataframe,bd.executeQueryDataFrame(consulta[0],consulta[1])])"""
    print(dataframe)
    
    
    
    for values in dataframe.values:
       try:
        datos=getHighLights.getHighLights(values[0]+"."+values[1])
        datos["fecha"]=dt.datetime.today()
        datos["stock"]=values[0]
        datos["exchange"]=values[1]
        placeholder = ", ".join(["%s"] * len(datos))
        execute="insert into highlights ({}) values ({})".format(",".join(datos.keys()),placeholder)
        print(values)
    
     
        bd.execute(execute,list(datos.values()))
       except Exception as e:
           print(e)
    
   
    
