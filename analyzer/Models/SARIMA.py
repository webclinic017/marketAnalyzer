#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 14:22:24 2022

@author: manuel
"""
import sys
sys.path.append("../getData")
sys.path.append("../../visualization")
sys.path.append("../functions")
import matplotlib.pyplot as plt
from scipy.stats import norm
import itertools
import math
import transformations
import bdStocks
import graficosJupyterNotebook as graficos
import numpy as np
import pandas as pd
import transformationsDataframes
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import acf
from statsmodels.tsa.stattools import pacf
from  statsmodels.tsa.stattools import arma_order_select_ic  as select_order
import scipy.stats
import statsmodels.stats.diagnostic
import statsmodels.api as sm
import math
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.arima_model import ARMA
from scipy.stats import norm
import math
from datetime import timedelta
import numpy as np
pd.set_option('display.max_rows', None)
import warnings
warnings.filterwarnings("ignore")
import configparser
config = configparser.ConfigParser()
config.read('../../config.properties')
log_dir_excepciones = "../../"+config.get('LOGS_DIR', 'excepciones')
import writer
nivel_confianza=float(config.get('EntrenamientoARIMA', 'nivel_confianza'))
todos=0.5
maximosOrders=3
ponderaciones=[float(e) for e in (config.get('EntrenamientoARIMA', 'ponderaciones').split(" "))]
def calcularDiferenciacion(serie_train,serie,periodicidad):
   
    #la hipotesis nula es que hay raiz unitaria
    diferenciacion=0
    diferenciacionEstacional=0
    estacional=True
    paramFuller=5
    
    
    adf=adfuller(serie_train)
    adf
    posibleEstacionaridad=False
    diferenciacion=0
    diferenciacionEstacional=0
    if adf[1]>nivel_confianza:
        posibleEstacionaridad=True
        
    if posibleEstacionaridad and estacional:
        serie1=pd.Series(serie_train).diff(periodicidad).dropna()
        adf1=adfuller(serie1)
    if posibleEstacionaridad:
        serie2=pd.Series(serie_train).diff(1).dropna()
        adf2=adfuller(serie2)
      
    if posibleEstacionaridad and estacional:
        print(adf1,adf2)
        niveles=[0.0001,0.001,0.01,0.05,0.1]
        for idx,nivel in enumerate(niveles[1:]):
            if adf1[1]<nivel:
                diferenciacionEstacional=1
                break
            elif adf2[1]<niveles[idx]:
                diferenciacion=1
                break
        if diferenciacion==0 and diferenciacionEstacional==0:
                serie3=pd.Series(serie1).diff(1).dropna()
                adf3=adfuller(serie3)
                
                if adf3[0]<adf3[4]["10%"]:
                    diferenciacionEstacional=1
                    diferenciacion=1
                else:
                    diferenciacionEstacional=1
                    diferenciacion=2
    
    
            
    #import matplotlib.pyplot as plt
    #plt.plot(serie)
    return diferenciacion,diferenciacionEstacional
    


class Modelo:
    def __init__(self,modelo,modeloExtendido,significacionP=None,significacionQ=None,significacionPEst=None,significacionQEst=None):
        
        self.modelo=modelo
        self.modeloExtendido=modeloExtendido
        self.errorTrain=modelo.mse
        self.errorTest=modeloExtendido.mse
        self.aicTrain=modelo.aic
        self.aicTest=modeloExtendido.aic
        
        self.aicTotal=ponderaciones[0]*self.aicTrain+ponderaciones[1]*self.aicTest
        self.errorTotal=ponderaciones[0]+self.errorTrain*ponderaciones[1]*self.errorTest
        self.significacionP=significacionP
        self.significacionQ=significacionQ
        self.significacionPEst=significacionPEst
        self.significacionQEst=significacionQEst
        
        
def crearYProbarModelo(serie_train,serie_test,ordenes=None,ordenesSeason=None,diferenciacion=0,diferenciacionSeason=0,periodo=4):
    modelo=None
    
    notSeasonalTrend="c"
    seasonalTrend="c"
    if diferenciacion>0:
        notSeasonalTrend="t"
    if diferenciacionSeason>0:
        seasonalTrend="t"
        if ordenesSeason is None:
            ordenesSeason=(0,0)
        
    if  ordenes is not None:
        ordenes=(ordenes[0],diferenciacion,ordenes[1])
    if  ordenes is not None and ordenesSeason is not None:
        ordenesSeason=(ordenesSeason[0],diferenciacionSeason,ordenesSeason[1],periodo)
    if ordenesSeason is not None:
           ordenesSeason=(ordenesSeason[0],diferenciacionSeason,ordenesSeason[1],periodo)
    if  ordenes is not None and ordenesSeason is not None:   
        s="c"
        if notSeasonalTrend=="t" or  seasonalTrend=="t":
            s="t"
       
        modelo=ARIMA(endog=serie_train,order=ordenes,seasonal_order=ordenesSeason)
    elif ordenes is not None:
        #modelo=ARIMA(endog=serie_train,order=ordenes,trend=notSeasonalTrend)
        modelo=ARIMA(endog=serie_train,order=ordenes)
    elif ordenesSeason is not None:
        #modelo=ARIMA(endog=serie_train,seasonal_order=ordenesSeason,trend= seasonalTrend)
        modelo=ARIMA(endog=serie_train,seasonal_order=ordenesSeason)
    
    if modelo is not None:    
        
        adj=modelo.fit()
        adj2=adj.extend(serie_test.to_numpy())
        
        return adj,adj2
def devolverEstadisticos(corr,pcorr,diferenciacion,serie):
    T=len(serie)-diferenciacion
    
    varSerie=1/T
    desv=math.sqrt(varSerie)
    
    return abs(corr)/desv,abs(pcorr)/desv
   
    
def estadisticosCorrelaciones(serie_train,diferenciacion,diferenciacionEstacional,periodicidad):  
    serie1=serie_train.copy()
    if diferenciacion and  diferenciacionEstacional:
        serie1=serie_train.diff(periodicidad).diff(1)
    elif diferenciacion:
        serie1=serie_train.diff(1)
    elif diferenciacionEstacional:
        serie1=serie_train.diff(periodicidad)
    serie1=serie1.dropna() 
    corr=acf(serie1,nlags=int(len(serie1)/3))
    pcorr=pacf(serie1,nlags=int(len(serie1)/3))
    nivel=norm.ppf(0.90)
    estCorr,estPcorr=(devolverEstadisticos(corr,pcorr,diferenciacion,serie1))
  
    #graficos.correlograma(corr,pcorr)
    return estCorr,estPcorr
def reglasSimple(serie_train,serie_test,estCorr,estPcorr,diferenciacion,diferenciacionSeason,periodicidad):
   adj,adj2=crearYProbarModelo(serie_train,serie_test,ordenes=(1,1),ordenesSeason=None,diferenciacion=1,diferenciacionSeason=0,periodo=periodicidad)
   modelos=[Modelo(adj,adj2)]
   return modelos
def reglas(serie_train,serie_test,estCorr,estPcorr,diferenciacion,diferenciacionSeason,periodicidad):
    nivel=norm.ppf(1-nivel_confianza/2)
    ordenesp={}
    ordenesq={}
    ordenespEstacional={}
    ordenesq={}
    ordenesqEstacional={}
    modelos={}
    if periodicidad<=7:
        multLags=4
        max_lag=periodicidad
    else:
        multLags=2
        max_lag=7
    
    for k in range(max_lag,0,-1): 
        if abs(estCorr[k])>nivel:

            ordenesp={i:norm.cdf(abs(estCorr[i]))-(1-norm.cdf(abs(estCorr[i]))) for i in range(1,k+1)}
            break

    for k in range(max_lag,0,-1): 
        if abs(estPcorr[k])>nivel:

            ordenesq={i:norm.cdf(abs(estPcorr[i]))-(1-norm.cdf(abs(estPcorr[i]))) for i in range(1,k+1)}
            break
    #ordenesq={1:0.5}

   
    
    L=min(len(estCorr)-1,  int( multLags*periodicidad))
    for k in range(L,periodicidad-1,-1): 
        if abs(estCorr[k])>nivel:
          
            l=int(k/periodicidad)
            if l not in  ordenesqEstacional.keys():
                ordenespEstacional[l]=norm.cdf(abs(estCorr[k]))-(1-norm.cdf(abs(estCorr[k])))
            

         
    for k in range(L,periodicidad-1,-1): 
     
       if abs(estPcorr[k])>nivel:
         
            l=int(k/periodicidad)
            if l not in  ordenesqEstacional.keys():
                ordenesqEstacional[l]=norm.cdf(abs(estPcorr[k]))-(1-norm.cdf(abs(estPcorr[k])))
    if todos!=None:
        for dic in ordenesp,ordenespEstacional:
            claves=list(dic.keys())
            if len(claves)>maximosOrders:
                claves.sort(key=lambda t:abs(estCorr[t]),reverse=True)
                print(claves)
                print(estCorr)
                
                tam=int(todos*len(claves))
               
                
                for e in claves[tam:]:
                    print(e)
                    dic.pop(e)
        for dic in ordenesq,ordenesqEstacional:
            claves=list(dic.keys())
            if len(claves)>maximosOrders:
                claves.sort(key=lambda t:abs(estPcorr[t]),reverse=True)
                tam=int(todos*len(claves))
                for e in claves[tam:]:
                    dic.pop(e)
    
    lista=[]
    listaEstacional=[]
    if len(ordenesp.keys())>0 and len(ordenesq.keys())>0:
        grid=np.meshgrid(list(ordenesp.keys()),list(ordenesq.keys()))
        lista=[[grid[0][k][i],grid[1][k][i]] for k in range(len(grid[0])) for i in range(len(grid[0][k]))]
    elif len(ordenesp.keys())>0:
         lista=[[e,0] for e in ordenesp.keys() ]

    elif len(ordenesq.keys())>0:
         lista=[[0,e] for e in ordenesq.keys() ]
   
    if len(ordenespEstacional.keys())>0 and len(ordenesqEstacional.keys())>0:
        grid=np.meshgrid(list(ordenespEstacional.keys()),list(ordenesqEstacional.keys()))
        listaEstacional=[[grid[0][k][i],grid[1][k][i]] for k in range(len(grid[0])) for i in range(len(grid[0][k]))]
    elif len(ordenespEstacional.keys())>0:
         listaEstacional=[[e,0] for e in ordenespEstacional.keys() ]

    elif len(ordenespEstacional.keys())>0:
         listaEstacional=[[0,e] for e in ordenesqEstacional.keys() ]
    modelos=[]
    print(lista)
    print(listaEstacional)
    
   
    for orde in lista:
        try:
            if (diferenciacionSeason==0) or (diferenciacionSeason>0 and orde[0]<periodicidad and orde[1]<periodicidad):
                adj,adj2=crearYProbarModelo(serie_train,serie_test,ordenes=orde,ordenesSeason=None,diferenciacion=diferenciacion,diferenciacionSeason=diferenciacionSeason,periodo=periodicidad)
                sigP=None
                sigQ=None
                if orde[0]!=0:
                    sigP=   ordenesp[orde[0]]
                if orde[1]!=0:
                    sigQ= ordenesq[orde[1]]
                modelo=Modelo(adj,adj2, sigP,sigQ)
                modelos.append(modelo)
        except Exception as e:
                writer.write(e,
                     log_dir_excepciones)
    
    for ordeEst in listaEstacional:
        try:
            adj,adj2=crearYProbarModelo(serie_train,serie_test,ordenes=None,ordenesSeason=ordeEst,diferenciacion=diferenciacion,diferenciacionSeason=diferenciacionSeason,periodo=periodicidad)  
            
            sigPEst=   ordenespEstacional[ordeEst[0]]
            sigQESt= ordenesqEstacional[ordeEst[1]]
            modelo=Modelo(adj,adj2, None,None, sigPEst,sigQESt)
            modelos.append(modelo)
        except Exception as e:
                writer.write(e,
                     log_dir_excepciones)
        if todos!=None:
            long=int(len(lista)/2)
        else:
            long=len(lista)
        for orde in lista[:long]:
            try:
                if orde[0]<4 and orde[1]<4:
                    #print(orde,ordeEst)
                    adj,adj2=crearYProbarModelo(serie_train,serie_test,ordenes=orde,ordenesSeason=ordeEst,diferenciacion=diferenciacion,diferenciacionSeason=diferenciacionSeason,periodo=periodicidad)
                    sigP=None
                    sigQ=None
                    if orde[0]!=0:
                        sigP=   ordenesp[orde[0]]
                    if orde[1]!=0:
                        sigQ= ordenesq[orde[1]]
                    sigPEst=None
                    sigQEst=None
                    if ordeEst[0]!=0:
                        sigPEst=   ordenespEstacional[ordeEst[0]]
                    if ordeEst[1]!=0:
                        sigQESt= ordenesqEstacional[ordeEst[1]]
                    modelo=Modelo(adj,adj2, sigP,sigQ, sigPEst,sigQEst)
                    modelos.append(modelo)
            except Exception as e:
                writer.write(e,
                     log_dir_excepciones)
                    
       
    
    
  
    return modelos


def obtenerDataFrameDeModelo(modelo:dict,clave:str):
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
    order=model.specification["order"]
    s_order=model.specification["seasonal_order"]
    inicio=max(order[0]+order[1],order[2]+order[1],s_order[0]*s_order[3]+s_order[1],s_order[2]*s_order[3]+s_order[1])
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
    #print("Fecha forecast: "+str(forecastOne.index[0])+ " real: "+str(serie_test.tail(1).values[0][0])+ " fitted in test: "+str(predicciones.tail(1).values[0])+ " forecast: "+ str(forecastOne.values[0]))
    #print(pred.tail(2))
    return pred