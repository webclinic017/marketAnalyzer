import os
os.chdir("../")
from database import  bd_handler
from database import  dabase_functions
from utils import work_dataframes
from functions import estacionaridadYCointegracion
if __name__ == "__main__":

    bd=bd_handler.bd_handler()
    data=bd.execute_consult_dataframe("select * from ratios_results")
    data["accion"]=data.exchange+data.stock
    data.set_index(["fecha","exchange","stock"],inplace=True)
    print(data)
    descr=dabase_functions.filter_by_description("natural energy","crude oil")
    print(len(descr))
    dataframe=None
    tendencias_largo=[]
    tendencias_corto=[]
    tendecias_medio=[]
    estacionarios_largo=[]
    estacionarios_corto=[]
    estacionarios_medio=[]
    for e in descr.values:
        exchange=e[0]
        stock=e[1]
        try:
            data_aux=data.loc[:,exchange,stock,:]
            fechas = work_dataframes.get_lookbacks_(data_aux, 4)


            for fecha in fechas:
                try:
                    data_aux_2 = data_aux.loc[data_aux.index > fecha]
                    resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data_aux)
                    data
                    array = [resultadosEst["hurst"], resultadosEst["results_adf"][1], resultadosEst["lambda_adf"]]
                    dataframe_resultados.loc[(column, fecha), ["hurst", "adf_test", "lambda"]] = array
                    print("Hurst: {}".format(str(resultadosEst["hurst"])))
                    print("Lambda adf: {}".format(resultadosEst["lambda_adf"]))
                    print("Test adf: {}".format(resultadosEst["results_adf"][1]))
                except Exception as e:
                    print(e)


        except Exception as e:
            print(e)






