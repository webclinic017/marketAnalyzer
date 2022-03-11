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
periodicidad=int(config.get('EntrenamientoARIMA', 'periodicidad'))
nivel_confianza=float(config.get('EntrenamientoARIMA', 'nivel_confianza'))
max_lag=int(config.get('EntrenamientoARIMA', 'max_lag'))
def calcularDiferenciacion(serie_train,serie):
   
    #la hipotesis nula es que hay raiz unitaria
    diferenciacion=0
    diferenciacionEstacional=0
    estacional=True
    paramFuller=5
    
    
    adf=adfuller(serie_train,maxlag=max_lag)
    adf
    posibleEstacionaridad=False
    diferenciacion=0
    diferenciacionEstacional=0
    if adf[1]>nivel_confianza:
        posibleEstacionaridad=True
        
    if posibleEstacionaridad and estacional:
        serie1=pd.Series(serie_train).diff(periodicidad).dropna()
        adf1=adfuller(serie1,maxlag=max_lag)
    if posibleEstacionaridad:
        serie2=pd.Series(serie_train).diff(1).dropna()
        adf2=adfuller(serie2,maxlag=max_lag)
      
    if posibleEstacionaridad and estacional:
        for k in ["1%","5%"]:
            if adf2[0]<adf1[4][k]:
                if adf1[0]<adf1[4][k]:
                    if adf1[1]<0.005:
                        diferenciacionEstacional=1
                        
                    else:
                        diferenciacion=1
                      
    
                else:
                    diferenciacion=1
                    
                break
        if diferenciacion==0 and diferenciacionEstacional==0:
                serie3=pd.Series(serie1).diff(1).dropna()
                adf3=adfuller(serie3,maxlag=max_lag)
                
                if adf3[0]<adf3[4]["5%"]:
                    diferenciacionEstacional=1
                    diferenciacion=1
                else:
                    serie_train=None
    
    
            
    #import matplotlib.pyplot as plt
    #plt.plot(serie)
    return diferenciacion,diferenciacionEstacional
    


class Modelo:
    def __init__(self,modelo,modeloExtendido,significacionP=None,significacionQ=None,significacionPEst=None,significacionQEst=None,ponderaciones=[0.6,0.4]):
        
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
        
        
def crearYProbarModelo(serie_train,serie_test,ordenes=None,ordenesSeason=None,diferenciacion=0,diferenciacionSeason=0,periodo=periodicidad):
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
        modelo=ARIMA(endog=serie_train,order=ordenes,trend=notSeasonalTrend)
    elif ordenesSeason is not None:
        modelo=ARIMA(endog=serie_train,seasonal_order=ordenesSeason,trend= seasonalTrend)
    if modelo is not None:    
        
        adj=modelo.fit()
        adj2=adj.extend(serie_test.to_numpy())
        
        return adj,adj2
def devolverEstadisticos(corr,pcorr,diferenciacion,serie):
    T=len(serie)-diferenciacion
    
    varSerie=1/T
    desv=math.sqrt(varSerie)
    
    return abs(corr)/desv,abs(pcorr)/desv
   
    
def estadisticosCorrelaciones(serie_train,diferenciacion,diferenciacionEstacional):  
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
def reglas(serie_train,serie_test,estCorr,estPcorr,diferenciacion,diferenciacionSeason):
    nivel=norm.ppf(1-nivel_confianza/2)
    ordenesp={}
    ordenesq={}
    ordenespEstacional={}
    ordenesq={}
    ordenesqEstacional={}
    modelos={}
    for k in range(max_lag,0,-1): 
        if abs(estCorr[k])>nivel:

            ordenesp={i:norm.cdf(abs(estCorr[i]))-(1-norm.cdf(abs(estCorr[i]))) for i in range(1,k+1)}
            break

    for k in range(max_lag,0,-1): 
        if abs(estPcorr[k])>nivel:

            ordenesq={i:norm.cdf(abs(estPcorr[i]))-(1-norm.cdf(abs(estPcorr[i]))) for i in range(1,k+1)}
            break
        #ordenesq={1:0.5}
    v=4
    L=min(len(estCorr)-1,v*periodicidad)  
    for k in range(L,periodicidad-1,-1): 
        if abs(estCorr[k])>nivel:
          
            l=int(k/periodicidad)
            if l not in  ordenesqEstacional.keys():
                ordenespEstacional[l]=norm.cdf(abs(estCorr[k]))-(1-norm.cdf(abs(estCorr[k])))
            

         
    for k in range(L,periodicidad-1,-1): 
     
       if abs(estPcorr[k])>nivel:
         
            l=int(k/periodicidad)
            if l not in  ordenesqEstacional.keys():
                ordenesqEstacional[l]=norm.cdf(abs(estCorr[k]))-(1-norm.cdf(abs(estCorr[k])))
          
        #ordenesq={1:0.5}
   
        
        
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
        except Exception as e:
                writer.write(e,
                     log_dir_excepciones)
        for orde in lista:
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