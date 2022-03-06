#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 11:04:14 2022

@author: manuel
"""
import configparser
config = configparser.ConfigParser()
config.read('../../config.properties')
TAM= config.get('Entrenamiento', 'tam_train')
CLASSOFICATION_METRIC=config.get('Entrenamiento', 'classification_metric')
cv=int(config.get('Entrenamiento', 'cv'))
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.decomposition import PCA
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import LinearSVC
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV
from sklearn.model_selection import cross_val_score,cross_val_predict
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score, recall_score,f1_score,accuracy_score
from sklearn.metrics import roc_curve
from sklearn.metrics import precision_recall_curve
from sklearn import datasets
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import LinearSVC
from sklearn.model_selection import  train_test_split
import numpy as np
max_deph=np.arange(1,7,1)
n_estimators=np.arange(1,10,1)
learning_rate=np.arange(0.001,1,0.005)
min_samples_split=np.arange(1,10,1)
C=np.arange(0.1,100,0.5)
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


#los siguientes modelos seleccionan parametros con randomSearch
def get_train_test(X,y,train_size=TAM,shuffle= False):
     X_train, X_test, y_train, y_test = train_test_split(
                                X,
                                y,
                                train_size   = float(train_size),
                                random_state = 1234,
                                shuffle      = shuffle
                            )
     return X_train, X_test, y_train, y_test
    
def gradientBoostingClassify(X_train,y_train,X_test,y_test):
   
    gbrt = GradientBoostingClassifier()
    param_grid=[{"max_depth":max_deph,"n_estimators":n_estimators,"learning_rate":learning_rate}]
    grid_search = RandomizedSearchCV(gbrt ,param_grid, cv=cv,
    scoring=CLASSOFICATION_METRIC)
    grid_search.fit(X_train,y_train)
    params=grid_search.best_params_
    gbrt = GradientBoostingClassifier(max_depth=params['max_depth'], n_estimators=params['n_estimators'], learning_rate=params['learning_rate'])
    gbrt.fit(X_train,y_train)
    y_pred=gbrt.predict(X_test)
    p=precision_score(y_test,y_pred,labels=[False,True],average=None)
    r=recall_score(y_test,y_pred,labels=[False,True],average=None)
    f1=f1_score(y_test,y_pred,labels=[False,True],average=None)
    ac=accuracy_score(y_test,y_pred)
    return gbrt,p,r,f1,ac,params
    
def AdaBoostingClassify(X_train,y_train,X_test,y_test):
   
    gbrt =  AdaBoostClassifier()
    param_grid=[{"n_estimators":n_estimators,"learning_rate":learning_rate}]
    grid_search = RandomizedSearchCV(gbrt ,param_grid, cv=cv,
    scoring=CLASSOFICATION_METRIC)
    grid_search.fit(X_train,y_train)
    params=grid_search.best_params_
    gbrt = AdaBoostClassifier(n_estimators=params['n_estimators'], learning_rate=params['learning_rate'])
    gbrt.fit(X_train,y_train)
    y_pred=gbrt.predict(X_test)
    p=precision_score(y_test,y_pred,labels=[False,True],average=None)
    r=recall_score(y_test,y_pred,labels=[False,True],average=None)
    f1=f1_score(y_test,y_pred,labels=[False,True],average=None)
    ac=accuracy_score(y_test,y_pred)
    return gbrt,p,r,f1,ac,params


def RandomForestClassify(X_train,y_train,X_test,y_test):
   
    gbrt = RandomForestClassifier()
    param_grid=[{"max_depth":max_deph,"n_estimators":n_estimators,"min_samples_split":min_samples_split}]
    grid_search = RandomizedSearchCV(gbrt ,param_grid, cv=cv,
    scoring=CLASSOFICATION_METRIC)
    grid_search.fit(X_train,y_train)
    params=grid_search.best_params_
    gbrt = RandomForestClassifier(max_depth=params['max_depth'], n_estimators=params['n_estimators'],min_samples_split=params['min_samples_split'])
    gbrt.fit(X_train,y_train)
    y_pred=gbrt.predict(X_test)
    p=precision_score(y_test,y_pred,labels=[False,True],average=None)
    r=recall_score(y_test,y_pred,labels=[False,True],average=None)
    f1=f1_score(y_test,y_pred,labels=[False,True],average=None)
    ac=accuracy_score(y_test,y_pred)
    return gbrt,p,r,f1,ac,params


def DecissionTreeClassify(X_train,y_train,X_test,y_test):
   
    gbrt =  DecisionTreeClassifier()
    param_grid=[{"max_depth":max_deph,"min_samples_split":min_samples_split}]
    grid_search = RandomizedSearchCV(gbrt ,param_grid, cv=cv,
    scoring=CLASSOFICATION_METRIC)
    grid_search.fit(X_train,y_train)
    params=grid_search.best_params_
    gbrt = DecisionTreeClassifier(max_depth=params['max_depth'],min_samples_split=params['min_samples_split'])
    gbrt.fit(X_train,y_train)
    y_pred=gbrt.predict(X_test)
    p=precision_score(y_test,y_pred,labels=[False,True],average=None)
    r=recall_score(y_test,y_pred,labels=[False,True],average=None)
    f1=f1_score(y_test,y_pred,labels=[False,True],average=None)
    ac=accuracy_score(y_test,y_pred)
    return gbrt,p,r,f1,ac,params

def LinearSVMClassify(X_train,y_train,X_test,y_test):
    gbrt =  LinearSVC()
    param_grid=[{"C":C}]
    grid_search = RandomizedSearchCV(gbrt ,param_grid, cv=cv,
    scoring=CLASSOFICATION_METRIC)
    grid_search.fit(X_train,y_train)
    params=grid_search.best_params_
    gbrt = LinearSVC(C=params['C'])
    gbrt.fit(X_train,y_train)
    y_pred=gbrt.predict(X_test)
    p=precision_score(y_test,y_pred,labels=[False,True],average=None)
    r=recall_score(y_test,y_pred,labels=[False,True],average=None)
    f1=f1_score(y_test,y_pred,labels=[False,True],average=None)
    ac=accuracy_score(y_test,y_pred)
    return gbrt,p,r,f1,ac,params
    
def LogisticRegressionClassify(X_train,y_train,X_test,y_test):
    log_reg = LogisticRegression()
    log_reg.fit(X_train, y_train)
    #y_pred = cross_val_predict(gbrt , X, y, cv=3)
    y_pred=log_reg.predict(X_test)
    p=precision_score(y_test,y_pred,labels=[False,True],average=None)
    r=recall_score(y_test,y_pred,labels=[False,True],average=None)
    f1=f1_score(y_test,y_pred,labels=[False,True],average=None)
    ac=accuracy_score(y_test,y_pred)
    params={}
    return log_reg,p,r,f1,ac,params