#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 10:26:26 2022

@author: manuel
"""
def pasarAMensual(dataframe):
    indice1=dataframe.groupby([dataframe.index.month,dataframe.index.year]).head(1)
    indice1["fecha"]=indice1.index
    indice1["fecha"]=indice1["fecha"].transform(lambda x:x.replace(day=1))
    indice1.set_index("fecha",inplace=True,drop=True)
    indice1
    return indice1