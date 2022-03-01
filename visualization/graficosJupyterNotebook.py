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

import transformations
pct_change=transformations.pct_change 


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
