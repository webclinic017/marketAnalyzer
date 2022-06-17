import os
os.chdir("../../")
import matplotlib.pyplot as plt
from numpy.random import  normal
import pandas as pd
import math
import numpy as np
from statsmodels.tsa.stattools import pacf,acf
from functions.estacionaridad import hurst
from functions import stastmodels_functions

def generate_ruido(media,scale):
    return normal(media,scale,1)[0]

def plot_series(serie,title):
    plt.figure(figsize=(10,10))
    plt.plot(serie.index,serie)
    plt.title(title)
    plt.show()
def random_walk(n=1000,var=1,cte=0):

    a=[]
    a.append(generate_ruido(0,var))
    for i in range(1,n):
        ruido=generate_ruido(0,var)
        a.append(cte+a[i-1]+ruido)

    return pd.Series(a,index=range(0,n))


def geometric_random_walk(n=1000,var=1,cte=0):

    a=[]
    a.append(generate_ruido(0,var))
    for _ in range(1,n):
        ruido=generate_ruido(0,var)
        anterior_log=math.log(a[-1])
        actual_log=anterior_log+ruido
        actual=math.exp(actual_log)

        a.append(cte+actual)

    return pd.Series(a,index=range(0,n))
def estacinario(n=1000,var=1,cte=0,coeficiente=1):

    a=[]
    a.append(generate_ruido(0,var))
    for i in range(1,n):
        ruido=generate_ruido(0,var)
        a.append(cte+coeficiente*a[i-1]+ruido)

    return pd.Series(a,index=range(0,n))


def arma(n=1000,var=1,cte=0,coeficientes_ar=None,coeficientes_ma=None):

    a=np.zeros(n)
    ruidos=np.zeros(n)
    ruido=generate_ruido(0,var)

    ruidos[0]=(ruido)
    a[0]=(cte+ruido)
    for i in range(1,n):
        k_ma=min(i,len(coeficientes_ma))
        k_ar=min(i,len(coeficientes_ar))
        aux_ma=aux_ar=0
        if len(coeficientes_ma)>0:
            aux_ma=((ruidos[i-k_ma:i])*(coeficientes_ma[-k_ma:])).sum().reshape(1,)
        if len(coeficientes_ar) > 0:
            aux_ar =((a[i - k_ar:i]) * (coeficientes_ar[-k_ar:])).sum().reshape(1,)
        aux_ma=float(aux_ma)
        aux_ar = float(aux_ar)
        ruido=generate_ruido(0,var)
        ruidos[i]=ruido
        a[i]=(cte+aux_ma+aux_ar+ruido)

    return pd.Series(a,index=range(0,n))

def correlograma(serie):
    corr = acf(serie, nlags=20,fft=False)
    pcorr = pacf(serie, nlags=20)
    for i, a in enumerate([corr[1:], pcorr[1:]]):
        fig, ax = plt.subplots(figsize=(15, 4))
        # plt.locator_params(axis="x", nbins=len(a))
        ax.bar(range(1, len(a) + 1), a)
        ax.set_xticks(range(1, len(a) + 1))
        ax.xaxis.set_tick_params(rotation=80)

        texto = "Autocorrelacion" if i == 0 else "Autocorrelacion parcial"
        plt.xlabel("Lag")
        plt.ylabel("Correlacion")
        plt.title(texto)
        plt.show()


def raices_polinomios(coeficientes):


    # a1*x^2 + a2*x + a3

    if len(coeficientes)==2:
           a1 = coeficientes[0]
           a2 = coeficientes[1]
           termino_raiz=a2**2-4*a1
           if(termino_raiz>=0):
            raiz=math.sqrt(termino_raiz)
           else:
            raiz = math.sqrt(-termino_raiz)

            raiz=complex(0,raiz)


           resultado1=(-a2+raiz)/(-2*a1)
           resultado2 = (-a2 - raiz) / (-2 * a1)
           return abs(resultado1),abs(resultado2)

    elif len(coeficientes)==1:
        return -1/coeficientes[0],0




if __name__=="__main__":

    do_arma=False
    random_walk_=random_walk(1000,1,0.1)
    plot_series(random_walk_,"Random walk")
    geometric_random_walk_ = random_walk(1000, 1, 0.1)
    plot_series(geometric_random_walk_, "Geometric random walk")
    estacinario_= estacinario(1000, 1, 0,1.)
    ar_coef=np.array([0.00,1.])
    am_coef=np.array([0])
    arma_=arma(300,1,0,ar_coef,am_coef)

    plot_series(estacinario_, "Estacinario")
    plot_series(arma_, "VARMA")
    if min(arma_)<0:
        arma_=arma_-2*min(arma_)
    print(min(arma_))

    data=pd.read_csv("data/raw/energies/dayly_energy.csv")
    arm=data.loc[:,"MI_TOT"].dropna()

    print(hurst.hurst_exponent(arm, method="all", max_chunksize=100, min_chunksize=8, num_chunksize=1))
    res=(stastmodels_functions.adfuller_(arma_))
    print(res)
    suma=0
    u=0
    if do_arma:
        for k in range(100):
            arma_ = arma(1000, 1, 0, ar_coef, am_coef)
            a= hurst.hurst_exponent(arma_, method="all", max_chunksize=200, min_chunksize=8, num_chunksize=5)
            suma+=(len(np.where(np.array(a)>0.5)[0]))
            est,_=stastmodels_functions.adfuller_(arma_)
            if est[1]<0.1:
               u += 1
        print(suma/100,u/100)

    correlograma(arma_)
