#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 13:24:08 2022

@author: manuel
"""

#parte para obtener los nombre de los stocks o el dataframe con los stocks filtrados
import sys
sys.path.append("../getData")
sys.path.append("../../analyzer/mlflow")
sys.path.append("../../analyzer/getData")
sys.path.append("../../analyzer/functions")
sys.path.append("../../visualization")
import graficosJupyterNotebook as graficos
import bdStocks
import transformations
import obtenerModelos
import math
import pandas as pd
import datetime as dt
import time
import numpy as np
import transformationsDataframes
unoAuno=False
BACKTESING = True
EXPERIMENT_ID=2
inicio=20
numOps=20
semilla=10
paramSubida=1.5
fechaI="2016-12-01"
periodoOperacion="3M"
fechaI = dt.datetime.strptime(fechaI, '%Y-%m-%d')
fechaF=dt.datetime.today().replace(day=1)
periodoIndice="3M"
column="netIncome"
column2="Adjusted_close"
filt = {"broker": "degiro", "exchanges": ["US"]}
bd = bdStocks.getData()
tiempo1=time.time()
parametroCorrelacion=0.4
if unoAuno:
    fundamental,stocksFundamental = bd.obtenerStocksStrategy(
        filt, "fundamental", columnas=["netIncome"])
    precios,stocksPrecios = bd.obtenerStocksStrategy(
        filt, "precios", columnas=["Adjusted_close"])
    print([e for e in stocksPrecios  if  e not  in stocksFundamental ])
    stocks=set.intersection(set(stocksPrecios), set(stocksFundamental))
else:
    names=bd.obtenerStocksStrategy(
    filt, "fundamental", columnas=["netIncome"],namesOnly=True)
tiempo2=time.time()
print(tiempo2-tiempo1)
ob = obtenerModelos.ObtenerModelos()
#%%
#parte si queremos obtener el modelo
if unoAuno:
    modelos={}
    
    stocksModels=[]
    for stock in stocks:
        try:
            ex=stock.split("_")[0]
            st=stock.split("_")[1]
            data=bd.getDataByStock(tipo="fundamental", exchange=ex,stock=st,columnas=["netIncome"])
            nas,minimo,model,model2,fechaI=ob.getModelo(st,ex,"netIncome",data,"3M")
            #print(model2.predict())
            stocksModels.append(stock)
        except Exception as e:
            pass
#%%
modelos=None
if not unoAuno: 
    
    modelos=ob.getModelos(filt["exchanges"][0],column,EXPERIMENT_ID,periodoIndice)
    if len(filt["exchanges"])>0:
        for e in filt["exchanges"][1:]:
                
                 aux=ob.getModelos(e,column,EXPERIMENT_ID,periodoIndice)
                 modelos.update(aux)
#%%
#obtencion de precios para operar cuando no utilizamos predicciones de precios
def obtenerPrecios(exchange,stock):
    #obtenemos los datos del stock
        data=bd.getDataByStock("precios",exchange,stock,bd=False,columnas=[column2])
        #dividimos por 1M y eliminamos nas
        data=transformationsDataframes.pasarAMensual(data)
        data=data.interpolate(method='linear')
        data=data.dropna()

        #pasamos a mensual y rellenamos el indice
        data=transformationsDataframes.pasarAMensual(data)
        all_days = pd.date_range(data.index[0], data.index[-1],freq="1M",normalize=True)
        all_days=all_days.map(lambda x:x.replace(day=1))
        ultimaFecha=data.index[-1]
        ultimoValor=data.iloc[-1] 
        data=data.reindex(all_days)
        if data.index[-1]!=ultimaFecha:
                    data.loc[ultimaFecha]=ultimoValor
        return data

dataframe=None
dataframePrecios=None
for e,modelo in modelos.items():
    dataframe1=obtenerPrecios(e.split("_")[0],e.split("_")[1])
   
    dataframe1["stock"]=e
    dataframe1.set_index(['stock'], append=True,drop=True,inplace=True)
  
    dataframePrecios1=dataframe1.copy()
    dataframe1=dataframe1.pct_change(1)
    if dataframe is None:
        dataframe=dataframe1
        dataframePrecios= dataframePrecios1
    else:
        dataframe=pd.concat([dataframe,dataframe1])
        dataframePrecios=pd.concat([dataframePrecios,dataframePrecios1])
#%%
#ejemplo de como utilizar los datos de los modelos
def trabajarConModelo(modelo:dict,clave:str):
    exponencial=modelo["exponential"]
    minimo=modelo["translate"]
    serie_train=modelo["serie_train"]
    serie_test=modelo["serie_test"]
    model=modelo["modelo"]
    if exponencial :
                     if minimo<0:
                         serie_test1=serie_test-minimo*2
                     elif minimo==0:
                         serie_test1=serie_test+10
                     else:
                          serie_test1=serie_test.copy()
                         
                     boxcox=transformations.boxcox
    
                     
                     serie_test1=(serie_test1).applymap(lambda x:(boxcox(0,x)))
    else:
                    serie_test1=serie_test.copy()
    extendido=model.extend(serie_test1 )  
    segundo=extendido.fittedvalues
    forecastOne=extendido.forecast(1)
    #segundo.loc[forecastOne.index[0]]=forecastOne.iloc[0]
    segundo=pd.concat([segundo, forecastOne])
    pred=model.get_prediction(inicio,len(serie_train)-1).summary_frame()
   
    dataframePred=model.get_forecast(len(serie_test)+1).summary_frame()
    dataframePred["fitted"]=segundo
    pred["fitted"]=model.fittedvalues
    
    if  exponencial:
                
                 aux=minimo if minimo<0 else 0
               
                 predicciones=segundo.map(lambda x:(math.exp(x)+2*aux))
                 forecastOne= forecastOne.map(lambda x:(math.exp(x)+2*aux))
                 dataframePred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]]=dataframePred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]].applymap(lambda x:(math.exp(x)+2*aux))
                 pred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]]=pred.loc[:,["fitted","mean","mean_ci_lower","mean_ci_upper"]].applymap(lambda x:(math.exp(x)+2*aux))
                 dataframePred.loc[:,["mean_se"]]=dataframePred.loc[:,["mean_se"]].applymap(lambda x:(math.exp(x)))
                 pred.loc[:,["mean_se"]]=pred.loc[:,["mean_se"]].applymap(lambda x:(math.exp(x)))
    else:
        predicciones=segundo
    dataframePred["real"]=serie_test
    pred["real"]=serie_train
    
   
    
    pred=pd.concat([pred,dataframePred])
    pred["stock"]=clave
    pred.set_index(['stock'], append=True,drop=True,inplace=True)
    print("Fecha forecast: "+str(forecastOne.index[0])+ " real: "+str(serie_test.tail(1).values[0][0])+ " fitted in test: "+str(predicciones.tail(1).values[0])+ " forecast: "+ str(forecastOne.values[0]))
    print(pred.tail(2))
    return pred

            
                #obtenemos intervalos de predicciones
claves=list(modelos.keys())
n=len(claves)
no=[e for e in names if  e not  in claves ]
print(len(no),len(claves),len(names))
#%%
dataframePredicciones=None
eliminados=[]
for clave,modelo in modelos.items():
    try:
        print(clave)
        #print(modelo["modelo"].predict().index[-1])
        dataframeAux=trabajarConModelo(modelo,clave)
        if dataframePredicciones is None:
            dataframePredicciones=dataframeAux
        else:
            dataframePredicciones=pd.concat([dataframePredicciones,dataframeAux])
    except Exception as e:
        eliminados.append(clave)
        
        print(e)
for clave in eliminados:
        modelos.pop(clave)
#%%
def calcularCorrelaciones():
    dicc={}
    for clave in modelos.keys():
        print(clave)
        data1=dataframePrecios.loc[(dataframePrecios.index.get_level_values(1)==clave)&(dataframePrecios.index.get_level_values(0)<=fechaI)]
        data1=transformationsDataframes.pasarAtrimestres(data1.droplevel(level=1))
        
        data2=dataframePredicciones.loc[(dataframePredicciones.index.get_level_values(1)==clave)&(dataframePredicciones.index.get_level_values(0)<=fechaI)].droplevel(level=1)
       
        dicc[clave]=data1[column2].corr(data2["real"])
    return dicc
dicCorrelaciones=calcularCorrelaciones()        

#%%
print(len(dicCorrelaciones)) 
#%%
time_range = pd.date_range(fechaI, fechaF,freq=periodoOperacion,normalize=True)
time_range=time_range.map(lambda x:x.replace(day=1))
np.random.seed(semilla)
semilla+=1
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
def reglas(time_range,i,claves,dataframePredicciones,dataframe,modelo):
    fechaAnt5=time_range[i]
    fechaAnt4=time_range[i+1]
    fechaAnt3=time_range[i+2]
    fechaAnt2=time_range[i+3]
    fechaAnt=time_range[i+4]
    fecha=time_range[i+5]
    clavesComp=[]
    clavesCompDict={}
    for c in claves:
        try:
            if modelos[c]["modelo"].specification["seasonal_periods"]==0:
                if dicCorrelaciones[c]>parametroCorrelacion:
                      fund1=float(dataframePredicciones.loc[fechaAnt,c]["fitted"])
                      fund2=float(dataframePredicciones.loc[fechaAnt2,c]["fitted"])
                      prez1=float(dataframePrecios.loc[fechaAnt,c]["Adjusted_close"])
                      prez2=float(dataframePrecios.loc[fechaAnt2,c]["Adjusted_close"])
                      
                      
                      if  (fund1>0 and fund2>0 and fund1>fund2*paramSubida):
                          clavesComp.append(c)
                          print(fund1,fund2,prez1,prez2)
                          clavesCompDict[c]=fund1/fund2/prez1/prez2
                
            elif modelos[c]["modelo"].specification["seasonal_periods"]==4:   
                if dicCorrelaciones[c]>parametroCorrelacion:
                    fund1=float(dataframePredicciones.loc[fechaAnt,c]["fitted"])
                    fund2=float(dataframePredicciones.loc[fechaAnt5,c]["fitted"])
                    prez1=float(dataframePrecios.loc[fechaAnt,c]["Adjusted_close"])
                    prez2=float(dataframePrecios.loc[fechaAnt5,c]["Adjusted_close"])
                   
                    if  (fund1>0 and fund2>0 and fund1>fund2*paramSubida):
                         clavesComp.append(c)
                         clavesCompDict[c]=fund1/fund2/prez1/prez2
          
                   
                    
        except Exception as e:
           pass
    clavesComp.sort(key=lambda t:clavesCompDict[t],reverse=True)
    clavesComp=clavesComp[0:numOps]
    suma=sum([e for i,e in clavesCompDict.items() if i in clavesComp])
    print(suma)
    cantidades={e:clavesCompDict[e]/suma for e in clavesComp}
    print(cantidades)
        #claves=np.array([e for e in claves1 if  dataframePredicciones.loc[fechaAnt,e]["fitted"]>abs(dataframePredicciones.loc[fechaAnt2,e]["fitted"])*2])
    return clavesComp,cantidades
    
def operarConFundamental(modelos,time_range):
    compras=[]
    cantidad=8000
    for i,fecha in enumerate(time_range[5:]):
        fechaAnt5=time_range[i]
        fechaAnt4=time_range[i+1]
        fechaAnt3=time_range[i+2]
        fechaAnt2=time_range[i+3]
        fechaAnt=time_range[i+3]
        fecha=time_range[i+4]
        claves1=np.array([e for e in modelos.keys() if dataframe.loc[dataframe.index.get_level_values(1)==e].index.get_level_values(0)[0]<=fecha\
                        and dataframePredicciones.loc[dataframePredicciones.index.get_level_values(1)==e].index.get_level_values(0)[0]<=fecha\
                      and dataframe.loc[dataframe.index.get_level_values(1)==e].index.get_level_values(0)[-1]>=fecha\
                        and dataframePredicciones.loc[dataframePredicciones.index.get_level_values(1)==e].index.get_level_values(0)[-1]>=fecha])
        
       
        #claves=np.array([e for e in claves1 if  dataframePredicciones.loc[fechaAnt,e]["fitted"]>abs(dataframePredicciones.loc[fechaAnt2,e]["fitted"])*2])
        #n=len(claves)
        #print(n)
    
        
        
        
        claves,cantidades=reglas(time_range,i,claves1,dataframePredicciones,dataframe,modelo)
        print("Numero de compras %s"%len(claves))
        n=len(claves)
        stocks=list(claves)
        #print(stocks)
        array=([[dataframe.loc[(fecha,stock),"Adjusted_close"],str(stock),fechaAnt] for stock in stocks if (fecha,stock) in dataframe.index and not np.isnan(dataframe.loc[(fecha,stock),"Adjusted_close"])])
        if n>0:
            suma=sum([e[0]for e in array])*cantidad/n
        else:
            suma=0
        compras=compras+[[e[1],e[2]] for e in array]
        if np.isnan(suma):
            suma=0
        cantidad+=suma
        print(cantidad,n,fecha)
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
            
    
    

if BACKTESING:
    cantidad,compras=operarConFundamental(modelos,time_range)
    #multiplesSimulacionesAleatorias(modelos,time_range)
    
#%%
stocks=np.unique([stock[0] for stock in compras])
import os

for stock in stocks:
    try:
        os.mkdir("./plots/"+stock.split("_")[0])
    except Exception as e:
        pass
    pred=trabajarConModelo(modelos[stock], stock)
    fecha=pred.index.get_level_values(0)
    pred.reset_index(inplace=True,drop=True)
    pred.index=fecha
    
    
    comprasAux=[[e[1],pred.loc[e[1],"fitted"]] for e in compras if e[0]==stock ]
    data1=dataframePrecios.loc[(dataframePrecios.index.get_level_values(1)==stock)]
    data1=transformationsDataframes.pasarAtrimestres(data1.droplevel(level=1))
    pred["prize"]=data1["Adjusted_close"]*np.mean(abs(pred["fitted"]))/np.mean(abs(data1["Adjusted_close"]))
    graficos.plot_forecast_dataframe(pred.loc[:,["prize","fitted","real"]],title=stock+" Seasonal: "+str(modelos[stock]["modelo"].specification["seasonal_periods"]),extraInfo=None,fileName="./plots/"+stock.split("_")[0]+"/"+stock,puntos=comprasAux)

    