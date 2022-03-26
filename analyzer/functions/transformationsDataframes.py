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
def agrupar(fecha):
    if fecha.month<3: 
        return fecha.replace(year=fecha.year-1,month=12)
    elif fecha.month<6:
        return fecha.replace(month=3)
    elif fecha.month<9:
        return fecha.replace(month=6)
    elif fecha.month<12:
        return fecha.replace(month=9)
    else:
        return fecha
def pasarAtrimestres(dataframe):
    indice1=dataframe.groupby([dataframe.index.month,dataframe.index.year]).head(1)
    indice1["fecha"]=indice1.index
    indice1.reset_index(drop=True,inplace=True)
    indice1["fecha"]=indice1["fecha"].transform(lambda x:x.replace(day=1))
    indice1["fecha"]=indice1["fecha"].transform(lambda x:agrupar(x))
    indice1=indice1.groupby(["fecha"], as_index=False).mean()
    indice1.set_index("fecha",inplace=True,drop=True)
    indice1
    return indice1

