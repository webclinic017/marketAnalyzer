from functions.estacionaridad import hurst
from functions import stastmodels_functions
from plots import plots_iniciales
from plots import other_plots
import pandas as pd



def analisis_estacionaridad(serie,tiempos=[50,4,2]):
    """
    Devuelve coeficientes de estacionaridad de la siere temporal (hurts y adf fuller)

    :param serie: serie temporal (univariante
    :type serie: pandas.Series
    :param tiempos: [max_chunksize,min_chunk_size,step]
    :type tiempos: list
    :return: dicccionario de coeficientes de estacionaridad
    :rtype: dict
    """
    a=tiempos


    exponentes1= hurst.hurst_exponent(serie, max_chunksize=a[0], min_chunksize=a[1], num_chunksize=a[2], method="all")


    adf_result,lamda=stastmodels_functions.adfuller_(serie)
    return {"hurst":exponentes1,"lambda_adf":lamda,"results_adf":{"statiscic:":adf_result[0],"p-value":adf_result[1]}}


def analizar_dataset(data):
   """
    Analiza y muestra graficamente los aspectos de estacionaridad de la serie temporal

   :param data:  dataframe con series temporales
   """
   dataframe_resultados=None
   for column in  data.columns:
        print(column)
        plots_iniciales.plot_serie_temporal(data, column,column,4,None)
        coeficientes=analisis_estacionaridad(data[column].dropna())
        columns=[]
        values=[]
        for key, value in coeficientes.items():

           if not isinstance(value,dict) and not isinstance(value,list):
                    columns.append(key)
                    values.append(value)
           elif isinstance(value,dict):
                for key2,value2 in value.items():
                    columns.append(key+"_"+key2)
                    values.append(value2)

        columns.append("nombre")
        values.append(column)

        if dataframe_resultados is None:
                   dataframe_resultados =pd.DataFrame(columns=columns)
        dataframe_resultados.loc[len(dataframe_resultados),columns]=values

   other_plots.grouped_bar_plots(dataframe_resultados,"nombre")






def analisis_univariante_arima(serie):
    #analisis univariante utilizando ARIMA

    pass
