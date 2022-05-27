#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 10:26:26 2022

@author: manuel
"""
"""
def pasarAMensual(dataframe):
    indice1=dataframe.groupby([dataframe.index.month,dataframe.index.year]).head(1)
    indice1["fecha"]=indice1.index
    indice1["fecha"]=indice1["fecha"].transform(lambda x:x.replace(day=1))
    indice1.set_index("fecha",inplace=True,drop=True)
    indice1
    return indice1"""
import math
import pandas as pd
def pasarAMensual(dataframe):
    if  "stock" in dataframe.index.names:
        indice1=dataframe.groupby([dataframe.index.get_level_values(0).month,dataframe.index.get_level_values(0).year,dataframe.index.get_level_values(1)]).tail(1)
        indice1["fecha"]=indice1.index.get_level_values(0)
        indice1["stock"]=indice1.index.get_level_values(1)
    else:
        indice1=dataframe.groupby([dataframe.index.month,dataframe.index.year]).tail(1)
        indice1["fecha"]=indice1.index
    
    indice1["fecha"]=indice1["fecha"].transform(lambda x:x.replace(day=1))
    if  "stock" in dataframe.index.names:
        indice1.set_index(["fecha","stock"],inplace=True,drop=True)
    else:
         indice1.set_index(["fecha"],inplace=True,drop=True)
    
        

    return indice1
def agrupar(fecha):
    if fecha.month<=3: 
        return fecha.replace(month=3)
    elif fecha.month<=6:
        return fecha.replace(month=6)
    elif fecha.month<=9:
        return fecha.replace(month=9)
    elif fecha.month<=12:
        return fecha.replace(month=12)
    else:
        return fecha
def pasarAtrimestres(dataframe):
    stock="stock" in dataframe.index.names
    if   stock:
        indice1=dataframe.groupby([dataframe.index.get_level_values(0).month,dataframe.index.get_level_values(0).year,dataframe.index.get_level_values(1)]).tail(1)
        indice1["fecha"]=indice1.index.get_level_values(0)
        indice1["stock"]=indice1.index.get_level_values(1)
    
    else:
        indice1=dataframe.groupby([dataframe.index.month,dataframe.index.year]).tail(1)
        indice1["fecha"]=indice1.index
    indice1.reset_index(drop=True,inplace=True)
    indice1["fecha"]=indice1["fecha"].transform(lambda x:x.replace(day=1))
    indice1["fecha"]=indice1["fecha"].transform(lambda x:agrupar(x))
    
    if not   stock:
        indice1=indice1.groupby(["fecha"], as_index=False).mean()
        indice1.set_index("fecha",inplace=True,drop=True)
    else:
       
        dicc={e:"mean" if indice1.dtypes[e]=="float" else "max" for e in indice1.columns}
        print(dicc)
        indice1=indice1.groupby(["fecha","stock"], as_index=False).agg(dicc)
        indice1.set_index(["fecha","stock"],inplace=True,drop=True)
     
    
    
    return indice1
def transformacionExponencialInversa(serie,minimo,coeficiente=2,valor0=10):
        if minimo==0:
            
            serie1=serie.map(lambda x:(math.exp(x)-valor0))
        else:
            aux=minimo if minimo<0 else 0
       
            serie1=serie.map(lambda x:(math.exp(x)+coeficiente*aux))
        
        return serie1
 
def transformationsDataframePrecios(data):
        data=pasarAMensual(data)
        data=data.interpolate(method='linear')
        data=data.dropna()
        all_days = pd.date_range(data.index[0], data.index[-1],freq="1M",normalize=True)
        all_days=all_days.map(lambda x:x.replace(day=1))
        ultimaFecha=data.index[-1]
        ultimoValor=data.iloc[-1] 
        data=data.reindex(all_days)
        if data.index[-1]!=ultimaFecha:
                    data.loc[ultimaFecha]=ultimoValor
        return data
if __name__=="__main__":

    import sys
    import pandas as pd
    sys.path.append("../getData")
    import bdStocks
    import transformationsDataframes as tD
    bd=bdStocks.getData()
    sql="select distinct(sector) from sectors"
    sql1="select distinct(exchange) from stocks"
    sectors=[e[0] for e in bd. executeQuery(sql)]
    exchanges=[e[0] for e in bd. executeQuery(sql1)]
    exchanges=["LSE","MC","XETRA","PA","US","TO","MI"]
    exchangesIndex={"BE":"DAX30","NEO":"SPTSX","LSE":"FTSE100","MC":"IBEX35","US":"SP500","XETRA":"DAX30","VX":"SMI","PA":"CAC40","TO":"SPTSX","MI":"FTSEMIB","AS":"AEX"}
    columnasPrecios=["Adjusted_close","sector"]
    columnasFundamental=["netIncome","ebitda","totalRevenue","totalAssets","sector"]
    bd=bdStocks.getData()
    bd.getDataByExchange(tipo="fundamental",exchange="MC",sector="Industrials",columnas=["netIncome"]).tail()
    exchange="MC"
    sector='Communication Services'
    fundamental=bd.getDataByExchange(tipo="fundamental",exchange=exchange,sector=sector,columnas=columnasFundamental)
    #fundamental.drop(["stock","sector"],axis=1,inplace=True)
    
    fundamental=tD.pasarAtrimestres(fundamental)
    precios=bd.getDataByExchange(tipo="precios",exchange=exchange,sector=sector,columnas=columnasPrecios)

    print(precios.resample("M"))
    precios=tD.pasarAMensual(precios)
    print(precios.loc[precios.index.get_level_values(1)=="TEF"].tail())
    print(fundamental.loc[fundamental.index.get_level_values(1)=="TEF"].sort_index().tail())
   
    
    agrupados=precios.groupby(level=1)
    medias=agrupados.mean()
    print(medias.tail())
    des=agrupados.std()
    preciosScaled=precios.copy()
    print(preciosScaled.index)
    preciosScaled["stock"]=preciosScaled.index.get_level_values(1)
    preciosScaled["fecha"]=preciosScaled.index.get_level_values(0)
    preciosScaled["Adjusted_close"]=preciosScaled.apply(lambda x:(x["Adjusted_close"]-medias.loc[x["stock"]]/des.loc[x["stock"]]),axis=1)