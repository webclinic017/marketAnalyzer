import os
os.chdir("../")

from config import load_config

config = load_config.config()
from functions.estacionaridad import estacionaridadYCointegracion
import datetime as dt
from utils.dataframes import work_dataframes
from plots import plots_iniciales
import pandas as pd

if __name__ == "__main__":
    #analisis de los seleccionados para energias
    data = pd.read_csv("./data/raw/energies/dayly_energy.csv", index_col=0)


    data.index = pd.to_datetime(data.index)
    fecha_minima = config["series_temporales"]["fecha_minima"]
    fecha_minima = dt.datetime.strptime(fecha_minima, "%Y-%m-%d")
    data = data.loc[data.index > fecha_minima]
    dataframe_resultados = pd.DataFrame(columns=["accion", "fecha", "hurst_rs","hurst_dma","hurst_dsod","hurst_diffusion", "adf_test", "lambda"])
    dataframe_resultados.set_index(["accion", "fecha"], inplace=True)

    for column in data.columns:
        data2 = data.loc[:, [column]].dropna()
        fechas = work_dataframes.get_lookbacks_(data2, 4)
        plots_iniciales.plot_serie_temporal(data2, column, column, num_plots=4,
                                                         archivo="reports/energies/" + column + ".jpg")



        for fecha in fechas:

                data_aux = data2.loc[data2.index > fecha]
                resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data_aux.loc[:, column])
                print(data_aux.tail())
                array=[]
                for h in resultadosEst["hurst"]:
                    array.append(h)
                array =array+ [resultadosEst["results_adf"]["p-value"], resultadosEst["lambda_adf"]]
                dataframe_resultados.loc[(column, fecha), ["hurst_rs","hurst_dma","hurst_dsod","hurst_diffusion", "adf_test", "lambda"]] = array
                print("Hurst: {}".format(str(resultadosEst["hurst"])))
                print("Lambda adf: {}".format(resultadosEst["lambda_adf"]))
                print("Test adf: {}".format(resultadosEst["results_adf"]))


    dataframe_resultados.to_csv("reports/energies/estacionaridad_univariante" + str(dt.date.today())+".csv")
