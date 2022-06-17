import os
os.chdir("../")
from utils.database import  bd_handler
from utils.database import database_functions
from utils.dataframes import work_dataframes
from functions.estacionaridad import estacionaridadYCointegracion
import pandas as pd
if __name__ == "__main__":

    bd= bd_handler.bd_handler("stocks")
    data=bd.execute_query_dataframe("select * from ratios_results")
    data["accion"]=data.exchange+data.stock
    data.set_index(["fecha","exchange","stock"],inplace=True)
    print(data)
    descr= database_functions.filter_by_description("natural energy", "crude oil",bd=bd)
    print(len(descr))

    dataframe_resultados = pd.DataFrame(columns=["accion", "fecha", "hurst_rs","hurst_dma","hurst_dsod","hurst_diffusion", "adf_test", "lambda"])
    dataframe_resultados.set_index(["accion", "fecha"], inplace=True)



    for e in descr.values:
        exchange=e[0]
        stock=e[1]

        data_aux=data.loc[:,exchange,stock,:]
        fechas = work_dataframes.get_lookbacks_(data_aux, 4)


        for fecha in fechas:

                resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data_aux.loc[:, "netIncome"].dropna())
                print(data_aux.tail())
                array = []
                for h in resultadosEst["hurst"]:
                    array.append(h)
                array = array + [resultadosEst["results_adf"]["p-value"], resultadosEst["lambda_adf"]]
                dataframe_resultados.loc[
                    (stock, fecha), ["hurst_rs", "hurst_dma", "hurst_dsod", "hurst_diffusion", "adf_test",
                                      "lambda"]] = array
                print("Hurst: {}".format(str(resultadosEst["hurst"])))
                print("Lambda adf: {}".format(resultadosEst["lambda_adf"]))
                print("Test adf: {}".format(resultadosEst["results_adf"]))






