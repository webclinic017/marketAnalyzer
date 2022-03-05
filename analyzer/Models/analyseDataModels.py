#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 11:04:14 2022

@author: manuel
"""

from sklearn.decomposition import PCA
def pca(dataframe,includeClose=False,columna="Close",n_components=None):
    if n_components is None:
        n_components=len(dataframe.columns)
        if includeClose==False:
            n_components-=1
    pca=PCA(n_components= n_components)
    if includeClose==False:
        X=pca.fit_transform(dataframe.drop(columna,axis=1))
    else:
        X=pca.fit_transform(dataframe)
        
    return X,pca