

from functions import hurst
from functions import stastmodels_functions





def analisis_estacionaridad(serie,tiempos=[50,4,2]):

    a=tiempos


    exponentes1=hurst.hurst_exponent(serie,max_chunksize=a[0],min_chunksize=a[1],num_chunksize=a[2],method="all")


    adf_result,lamda=stastmodels_functions.adfuller_(serie)
    return {"hurst":exponentes1,"lambda_adf":lamda,"results_adf":adf_result}








def analisis_univariante_arima(serie):
    #analisis univariante utilizando ARIMA

    pass
