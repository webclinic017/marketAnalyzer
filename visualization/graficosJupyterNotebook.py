#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 18:42:28 2022

@author: manuel
"""
import matplotlib.pyplot as plt
import seaborn
import math
import numpy as np
import matplotlib.dates as mdates
import sys
import matplotlib.pyplot as plt
from matplotlib import rcParams
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.nonparametric.smoothers_lowess import lowess
import numpy as np
import scipy.stats as stats
from pydataset import data


import transformations
pct_change=transformations.pct_change 
FIG_SIZE=(10,10)
POS_INFO=(0.03,0.9)
#morado es true
def scatterWithCOlor(X,y,cero=None,pendiente=None):
   
        
    fig=plt.figure(figsize=FIG_SIZE)
    plt.scatter(X.iloc[:,0],X.iloc[:,1],c=y)
    plt.xlabel(X.columns[0])
    plt.ylabel(X.columns[1])
    if cero is not None:
        limite=plt.xlim()
        inicio=[limite[0],(-cero-pendiente[0]*limite[0])/pendiente[1]]
        fin=[limite[1],(-cero-pendiente[0]*limite[1])/pendiente[1]]
        plt.plot([inicio[0],fin[0]],[inicio[1],fin[1]],"--")
    
def correlograma(corr,pcorr):
    import matplotlib.pyplot as plt
    fig=plt.figure(figsize=FIG_SIZE)
    plt.locator_params(axis="x", nbins=len(corr))
    plt.bar(range(len(corr)),corr)
    fig=plt.figure(figsize=FIG_SIZE)
    plt.locator_params(axis="x", nbins=len(corr))
    plt.bar(range(len(pcorr)),pcorr)

def linearplot(dataframe1,title=None,scale=False,*args):
    
    #seaborn.pairplot(X)
   
    fig=plt.figure(figsize=FIG_SIZE)
    plt.grid(True)
    ax1 = fig.add_subplot(111, projection='rectilinear')  # Engadimos Axes á figura (contén os elementos do debuxo, queremos unha matrix [1,2])
    columna1=args[0]
    dataframe=dataframe1.copy()
    
    dataframe=dataframe.loc[:,args]
    if scale:
        dataframe=(dataframe-dataframe.mean())/dataframe.std()
    #dataframe.loc[:,args]=dataframe.loc[:,args].transform(lambda x:x.pct_change())
    ax1.xaxis.set_minor_formatter(mdates.DateFormatter('%m'))
    ax1.plot(dataframe.index,dataframe[columna1],label=columna1, marker='o')
    if len(args)>1:
        for columna in args[1:]:
            ax1.plot(dataframe.index,dataframe[columna],label=columna, marker='o')
   
    if title is not None:
        plt.title(title)
    plt.legend()
    plt.show()
    
    
    
def plot_forecast(serie,dataframePred,fitted,title=None,scale=False,fileName=None,extraInfo=None,*args,):
    
    fig=plt.figure(figsize=FIG_SIZE)
    plt.grid(True)
    ax1 = fig.add_subplot(111, projection='rectilinear')  # Engadimos Axes á figura (contén os elementos do debuxo, queremos unha matrix [1,2])
    
    ax1.xaxis.set_minor_formatter(mdates.DateFormatter('%m'))
    
    
    ax1.plot(serie.index,serie,marker="o",label="real")
    for k in ("mean","mean_ci_upper","mean_ci_lower","real"):
        ax1.plot(dataframePred.index,dataframePred.loc[:,k],marker="o",label=k)
    
    ax1.plot(fitted.index,fitted,marker="o",label="fitted")
    ax1.legend()
    if title is not None:
        plt.title(title)
    if extraInfo is not None:
        plt.figtext(POS_INFO[0],POS_INFO[1],extraInfo)
    if fileName is not None:
        plt.savefig(fileName)
   
    
    
def linearplot_multiple_data(column,scale=False,title=None,*args):
    fig=plt.figure(figsize=FIG_SIZE)
    
    for dataframe1 in args:
        dataframe=dataframe1.copy()
        #seaborn.pairplot(X)
        
        
        plt.grid(True)
        #ax1 = fig.add_subplot(111, projection='rectilinear')  # Engadimos Axes á figura (contén os elementos do debuxo, queremos unha matrix [1,2])
      
        stock=np.unique(dataframe["stock"])[0]
       
        dataframe=dataframe.loc[:,[column]]
        if scale:
            dataframe=(dataframe-dataframe.mean())/dataframe.std()
        #dataframe[column]=dataframe[column].transform(lambda x:x.pct_change())
           
        plt.plot(dataframe.index,dataframe[column],label=stock, marker='o')
        
   
    if title is not None:
        plt.title(title)
    plt.legend()
    plt.show()


def residualsVSfitted( modeloAjustado):
    #analisis de los residuos
    residuals = modeloAjustado.resid
    fitted = modeloAjustado.fittedvalues
    smoothed = lowess(residuals,fitted)
    top3 = abs(residuals).sort_values(ascending = False)[:3]
    
    plt.rcParams.update({'font.size': 16})
    plt.rcParams["figure.figsize"] = (8,7)
    fig, ax = plt.subplots()
    ax.scatter(fitted, residuals, edgecolors = 'k', facecolors = 'none')
    ax.plot(smoothed[:,0],smoothed[:,1],color = 'r')
    ax.set_ylabel('Residuals')
    ax.set_xlabel('Fitted Values')
    ax.set_title('Residuals vs. Fitted')
    ax.plot([min(fitted),max(fitted)],[0,0],color = 'k',linestyle = ':', alpha = .3)
    
    for i in top3.index:
        ax.annotate(i,xy=(fitted[i],residuals[i]))
    
    plt.show()

def qqplot( modeloAjustado):
    import pandas as pd
    
    sorted_student_residuals = modeloAjustado.resid.sort_values(ascending = True)
    df1 = pd.DataFrame(sorted_student_residuals)
    df1.columns = ['sorted_student_residuals']
    df1['theoretical_quantiles'] = stats.probplot(df1['sorted_student_residuals'], dist = 'norm', fit = False)[0]
    rankings = abs(df1['sorted_student_residuals']).sort_values(ascending = False)
    top3 = rankings[:3]
    
    fig, ax = plt.subplots()
    x = df1['theoretical_quantiles']
    y = df1['sorted_student_residuals']
    ax.scatter(x,y, edgecolor = 'k',facecolor = 'none')
    ax.set_title('Normal Q-Q')
    ax.set_ylabel('Standardized Residuals')
    ax.set_xlabel('Theoretical Quantiles')
    ax.plot([np.min([x,y]),np.max([x,y])],[np.min([x,y]),np.max([x,y])], color = 'r', ls = '--')
    for val in top3.index:
        ax.annotate(val,xy=(df1['theoretical_quantiles'].loc[val],df1['sorted_student_residuals'].loc[val]))
    plt.show()


#graficos para metricas de clasificacion

def plot_roc_curve(fpr, tpr, label=None):
    fig=plt.figure(figsize=FIG_SIZE)
    plt.plot(fpr, tpr, linewidth=2, label=label)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.axis([0, 1, 0, 1])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    
    
def plot_precission_recall(precision, recall, label=None):
    fig=plt.figure(figsize=FIG_SIZE)
    plt.plot(recall,precision, linewidth=2, label=label)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.axis([0, 1, 0, 1])
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    
def plot_precision_recall_vs_threshold(precisions, recalls, thresholds):
    fig=plt.figure(figsize=FIG_SIZE)
    plt.plot(thresholds, precisions[:-1], "b--", label="Precision")
    plt.plot(thresholds, recalls[:-1], "g-", label="Recall")
    plt.xlabel("Threshold")
    plt.legend(loc="upper left")
    plt.ylim([0, 1])
    plt.show()
    
