import sys
sys.path.append("../getData")
sys.path.append("../../analyzer/mlflow")
sys.path.append("../../analyzer/Models")
sys.path.append("../../analyzer/getData")
sys.path.append("../../analyzer/functions")
sys.path.append("../../visualization")
import graficosJupyterNotebook as graficos
import bdStocks
import SARIMA
import transformations
import obtenerModelos
import math
import pandas as pd
import datetime as dt
import time
import numpy as np

import transformationsDataframes
BACKTESING = True
EXPERIMENT_ID=3
inicio=20
numOps=20
semilla=10
paramSubida=1.5
fechaI="2016-12-01"
fechaI = dt.datetime.strptime(fechaI, '%Y-%m-%d')
fechaF=dt.datetime.today().replace(day=1)
periodoIndice="M"
column="netIncome"
column2="Adjusted_close"
filt = {"broker": "degiro", "exchanges": ["MC"]}
bd = bdStocks.getData()
names=bd.obtenerStocksStrategy(
filt, "precios", columnas=[],namesOnly=True)
ob = obtenerModelos.ObtenerModelos()
print(names)
modelos=ob.getModelos(filt["exchanges"][0],column2,EXPERIMENT_ID,periodoIndice,tipo="precios",stocks=names)
if len(filt["exchanges"])>0:
        for e in filt["exchanges"][1:]:
                
                 aux=ob.getModelos(e,column2,EXPERIMENT_ID,periodoIndice,tipo="precios")
                 print(aux)
                 modelos.update(aux)
        

#%%
dataframe=None
dataframePrecios=None
for e,modelo in modelos.items():
    print(e)
    exchange=e.split("_")[0]
    stock=e.split("_")[1]
    dataframe1=bd.getDataByStock("precios",exchange,stock,bd=False,columnas=[column2])
    dataframe1=transformationsDataframes.transformationsDataframePrecios(dataframe1)
    dataframe1["stock"]=e
    dataframe1.set_index(['stock'], append=True,drop=True,inplace=True)
    print(dataframe1.tail())
    dataframePrecios1=dataframe1.copy()
    dataframe1=dataframe1.pct_change(1)
    if dataframe is None:
        dataframe=dataframe1
        dataframePrecios= dataframePrecios1
    else:
        dataframe=pd.concat([dataframe,dataframe1])
        dataframePrecios=pd.concat([dataframePrecios,dataframePrecios1])

#%%
dataframePredicciones=None
eliminados=[]
for clave,modelo in modelos.items():
    try:
        print(clave,modelo["modelo"].fittedvalues.index[-1])
        
        #print(modelo["modelo"].predict().index[-1])
        dataframeAux=SARIMA.obtenerDataFrameDeModelo(modelo,clave)
        if dataframePredicciones is None:
            dataframePredicciones=dataframeAux
        else:
            dataframePredicciones=pd.concat([dataframePredicciones,dataframeAux])
    except Exception as e:
         eliminados.append(clave)
   
for clave in eliminados:
       modelos.pop(clave)      
      
        
#%%       

def operarAleatorio(modelos,time_range):
    
    cantidad=8000
    for i,fecha in enumerate(time_range[1:]):
       
        claves=np.array([e for e in modelos.keys() if dataframe.loc[dataframe.index.get_level_values(1)==e].index.get_level_values(0)[0]<fecha\
                        and dataframePredicciones.loc[dataframePredicciones.index.get_level_values(1)==e].index.get_level_values(0)[0]<fecha])
        n=len(claves)
        #print(n)
        u=np.random.randint(0,n,numOps)
        stocks=claves[u]
        #print(stocks)
        suma=sum([dataframe.loc[(fecha,stock),"Adjusted_close"] for stock in stocks if (fecha,stock) in dataframe.index and not np.isnan(dataframe.loc[(fecha,stock),"Adjusted_close"])])*cantidad/numOps
        cantidad+=suma
        print(fecha,n,cantidad)
        
        #print(stocks)
    return cantidad

def operarConFundamental(modelos,time_range):
    compras=[]
    cantidad=8000
    for i,fecha in enumerate(time_range[5:]):
        fechaAnt5=time_range[i]
        fechaAnt4=time_range[i+1]
        fechaAnt3=time_range[i+2]
        fechaAnt2=time_range[i+2]
        fechaAnt=time_range[i+3]
        fecha=time_range[i+4]
        claves1=np.array([e for e in modelos.keys() if dataframe.loc[dataframe.index.get_level_values(1)==e].index.get_level_values(0)[0]<=fecha\
                        and dataframePredicciones.loc[dataframePredicciones.index.get_level_values(1)==e].index.get_level_values(0)[0]<=fecha\
                      and dataframe.loc[dataframe.index.get_level_values(1)==e].index.get_level_values(0)[-1]>=fecha\
                        and dataframePredicciones.loc[dataframePredicciones.index.get_level_values(1)==e].index.get_level_values(0)[-1]>=fecha])
        
       
        claves=[]
        for e in claves1:
           if (fechaAnt,e) in dataframePredicciones.index\
                  and (fechaAnt2,e) in dataframePredicciones.index and dataframePredicciones.loc[fecha,e]["fitted"]>abs(dataframePredicciones.loc[fechaAnt,e]["fitted"]):
                      claves.append(e)
                      if (fecha,e) in dataframe.index:
                          a=dataframe.loc[(fecha,e),"Adjusted_close"]
                          if a>1 :
                              print("-----------------")
                              print(e,a)
                              print("---------------------")
                          
       
        #claves=np.array([e for e in claves1 if (fechaAnt,e) in dataframePredicciones.index\
         #                and (fechaAnt2,e) in dataframePredicciones.index and dataframePredicciones.loc[fecha,e]["fitted"]>abs(dataframePredicciones.loc[fechaAnt,e]["fitted"])])
        
        #lista=np.random.randint(0,len(claves1),int(len(claves1)/2))
        #claves=[e for i,e in enumerate(claves1) if i in lista]
        
       
        stocks=list(claves)
        #lista=np.random.randint(0,len(stocks),2)
        #stocks=[e for i,e in enumerate(claves) if i in lista]
        #n=len(stocks)
        
        
        #print(stocks)
        array=([[dataframe.loc[(fecha,stock),"Adjusted_close"],str(stock),fechaAnt] for stock in stocks if (fecha,stock) in dataframe.index and not np.isnan(dataframe.loc[(fecha,stock),"Adjusted_close"])])
        n=len(array)
       
        #if n>0:
         #   suma=sum([e[0]for e in array])*cantidad/n
        #else:
         #   suma=0
        pct=sum([e[0]for e in array])/n
        print(pct)
        if n>0:
            suma=pct*cantidad
        else:
            suma=0
        
        compras=compras+[[e[1],e[2]] for e in array]
        if np.isnan(suma):
            suma=0
        cantidad+=suma
        u=[e[1] for e in array]
        print("###########")
        print(u)
        print("Pct %s, n %s, suma %s, cantidad %s ,fecha %s"%(pct,n,suma,cantidad,fecha))
        print("###########")
        #print(stocks)
    return cantidad,compras
               
def multiplesSimulacionesAleatorias(modelos,time_range):
    media=0
    semilla=20
    np.random.seed(semilla)
    veces=30
    semilla+=1
    for i in range(veces):
         media+=operarAleatorio(modelos,time_range)
         semilla+=1
    print(media/veces)
            
#%%
dataframePredicciones=dataframePredicciones.dropna()
time_range = pd.date_range(fechaI, fechaF,freq=periodoIndice,normalize=True)
time_range=time_range.map(lambda x:x.replace(day=1))
np.random.seed(semilla)
semilla+=1
cantidad,compras=operarConFundamental(modelos,time_range)       

#%%
stocks=np.unique([stock[0] for stock in compras])
import graficosJupyterNotebook as graficos
import os

for stock in stocks:
    try:
        os.mkdir("./plotsPrecios/"+stock.split("_")[0])
    except Exception as e:
       pass
  
    pred=SARIMA.obtenerDataFrameDeModelo(modelos[stock], stock)
    fecha=pred.index.get_level_values(0)
    pred.reset_index(inplace=True,drop=True)
    pred.index=fecha
    pred=pred.loc[pred.index.get_level_values(0)>=fechaI]
    
    
    comprasAux=[[e[1],pred.loc[e[1],"fitted"]] for e in compras if e[0]==stock ]
    data1=dataframePrecios.loc[(dataframePrecios.index.get_level_values(1)==stock)]
    data1=transformationsDataframes.pasarAtrimestres(data1.droplevel(level=1))
   
    graficos.plot_forecast_dataframe(pred.loc[:,["fitted","real"]],title=stock+" Seasonal: "+str(modelos[stock]["modelo"].specification["seasonal_periods"]),extraInfo=None,fileName="./plotsPrecios/"+stock.split("_")[0]+"/"+stock,puntos=comprasAux)
    
    #data1=dataframePrecios.loc[(dataframePrecios.index.get_level_values(1)==stock)].droplevel("stock")
    #comprasAux=[[e[1],data1.loc[e[1],"Adjusted_close"]] for e in compras if e[0]==stock ]
    #print(data1.tail())
    #graficos.plot_forecast_dataframe(data1,title=stock+" Seasonal: "+str(modelos[stock]["modelo"].specification["seasonal_periods"]),extraInfo=None,fileName="./plotsPrecios/"+stock.split("_")[0]+"/"+stock,puntos=comprasAux)
#%%
modelo=modelos["MC_ALC"]
serie_test=modelo["serie_test"]
import matplotlib.pyplot as plt
print(serie_test.tail(10))
plt.plot(serie_test.index,serie_test.Adjusted_close,marker="o")