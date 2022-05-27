#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 17:18:41 2022

@author: manuel
"""
#%%
import os
print(os.getcwd())
import pandas as pd
import sys
import numpy as np
sys.path.append("../../analyzer/getData")
import configparser
config = configparser.ConfigParser()
config.read('../../config.properties')
directorio=config.get('ARCHIVOS', 'archivos_h5s_precios')
print(directorio)
import bdStocks
bd= bdStocks.getData()
sectores=None
import datetime as dt
fechaI="2008-12-01"
fechaF="2022-06-01"
fechaI=dt.datetime.strptime(fechaI, '%Y-%m-%d')
fechaF=dt.datetime.strptime(fechaF, '%Y-%m-%d')
periodoIndice="3M"
import os
files=os.listdir(directorio)
h5s= pd.HDFStore(directorio+files[0], "r")
precios=h5s["data"]
precios["exchange"]=files[0].split("_")[-1].split(".")[0]
exchanges=[]
for file in files[1:]:
    h5s=pd.HDFStore(directorio+file, "r")
    dat_aux=h5s["data"]
    ex=file.split("_")[-1].split(".")[0]
    dat_aux["exchange"]=ex
    precios=pd.concat([precios,dat_aux])
    exchanges.append(ex)
precios.set_index("exchange",inplace=True,append=True)
sectores=bd.getSectors()
#%%
print(precios.tail(10))
print(exchanges)
#%%
if __name__=="__main__":
    print("Backtesting")
    
    datos=bd.executeQueryDataFrame("select fecha,stock,exchange,adjusted_close,per from ratios_results",None)
    datos["fecha"]=pd.to_datetime(datos["fecha"])
    datos=datos.loc[datos.exchange.isin(exchanges)]
    datos.set_index(["fecha","exchange","stock"],inplace=True)
    exchanges=np.unique(datos.index.get_level_values("exchange"))
#%%
    import numpy as np
    #print(datos.tail())
    time_range = pd.date_range(fechaI, fechaF,freq=periodoIndice,normalize=True)
    time_range=[e.replace(day=1) for e in time_range]
    beneficio=0
    cantidad=8000
    
    fechas=iter(time_range[:-1])
#%%
fecha=next(fechas)
data=datos.loc[(fecha,slice(None),slice(None))]
import matplotlib.pyplot as plt
import seaborn as sns


#print(data.shape)
indice=data.loc[(data.per>0)&(data.per<30)].head(100000)
print(fecha)
print(len(indice))
print(indice.groupby(level="exchange").count()/len(indice))
sns.displot(x=indice.index.get_level_values(1) ,bins=7,height=10,stat="count")
sns.displot(x=indice.index.get_level_values(1) ,bins=7,height=10,stat="probability")
#print(data.shape)
#%%
exchanges_aux=iter(exchanges)
#%%
e=next(exchanges_aux)
print(e)
#%%
dat_index=indice.loc[:,e,:]
   
dat_index_ordenado=dat_index.sort_values(by="per")
dat_index["accion"]=dat_index.index.get_level_values(1)+"_"+dat_index.index.get_level_values(2)
dat_index_ordenado["sector"]=dat_index["accion"].transform(lambda x:sectores[x])
#%%
print(dat_index_ordenado.tail(10))
plt.figure(figsize=(10,10))
sns.displot(x=dat_index_ordenado.sector,bins=len(np.unique(dat_index_ordenado.sector)),height=10,stat="count") 
plt.title(e)
#%%
beneficio=0
c=cantidad/len(indice)
for e in indice:
    try:
        datStock=precios.loc[(precios.index.get_level_values(0)>=fecha,e[2],e[1])].head(2)
        if len( datStock)==2:
                #print(datStock)
                #print(datStock.adjusted_close.pct_change(1)[1])
                cambio=datStock.Adjusted_close.pct_change(1)[1]
                if cambio<1:
                    a=c*cambio
                if not np.isnan(a) and not np.isinf(a):
                    beneficio+=a 
    except Exception as e:
        print(e)
 
  
cantidad+=beneficio
print(cantidad)
    
#%%
print(sys.getsizeof(datos)/1024**2)   

#%%
fechas=bd.executeQueryDataFrame("select report_date from calendarioResultados",None) 
#%%
fechas["report_date"]=pd.to_datetime(fechas.report_date)
fechas["dia"]=fechas["report_date"].transform(lambda x: x.month*30+int(x.day/10))
fech=fechas.groupby("dia").count()
print(fech.tail())
#%%
import matplotlib.pyplot as plt
import seaborn as sns
sns.barplot(x=fech.index, y=fech.report_date)
