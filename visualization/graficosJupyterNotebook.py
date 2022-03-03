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

def correlograma(corr,pcorr):
    import matplotlib.pyplot as plt
    fig=plt.figure(figsize=(13,5))
    plt.locator_params(axis="x", nbins=len(corr))
    plt.bar(range(len(corr)),corr)
    fig=plt.figure(figsize=(13,5))
    plt.locator_params(axis="x", nbins=len(corr))
    plt.bar(range(len(pcorr)),pcorr)

def linearplot(dataframe1,title=None,scale=False,*args):
    
    #seaborn.pairplot(X)
   
    fig=plt.figure(figsize=(13,5))
    plt.grid(True)
    ax1 = fig.add_subplot(111, projection='rectilinear')  # Engadimos Axes á figura (contén os elementos do debuxo, queremos unha matrix [1,2])
    columna1=args[0]
    dataframe=dataframe1.copy()
    
    dataframe=dataframe.loc[:,args]
    if scale:
        dataframe=(dataframe-dataframe.mean())/dataframe.std()
    #dataframe.loc[:,args]=dataframe.loc[:,args].transform(lambda x:x.pct_change())
    ax1.xaxis.set_minor_formatter(mdates.DateFormatter('%m'))
    ax1.plot(dataframe.index,dataframe[columna1],label=columna1)
    if len(args)>1:
        for columna in args[1:]:
            ax1.plot(dataframe.index,dataframe[columna],label=columna)
   
    if title is not None:
        plt.title(title)
    plt.legend()
    plt.show()
    
    
def linearplot_multiple_data(column,scale=False,title=None,*args):
    fig=plt.figure(figsize=(13,5))
    
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
           
        plt.plot(dataframe.index,dataframe[column],label=stock)
        
   
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
