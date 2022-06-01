import os
os.chdir("../")
from database import dabase_functions
from plots import plots_iniciales
import datetime as dt
if __name__=="__main__":
    fecha_corte="2010-01-01"
    stock="MC_ACX"
    nombre_serie = ["netIncome","totalrevenue","totalAssets","totalLiab"]
    #nombre_serie="netIncome"
    #nombre_serie="adjusted_close"
    freq="D"
    if isinstance(nombre_serie,str) and nombre_serie=="adjusted_close":
        data=dabase_functions.get_prize_or_fundamenal(stock.split("_")[0],
                                                      stock.split("_")[1],
                                                      freq=freq, type="precios",
                                                      columna="adjusted_close", col_name=None)

    elif isinstance(nombre_serie, str):
        data = dabase_functions.get_prize_or_fundamenal(stock.split("_")[0],
                                                        stock.split("_")[1],
                                                        freq=freq, type="fundamental",
                                                        columna=nombre_serie, col_name=None)
    elif isinstance(nombre_serie, list):
        data= dabase_functions.get_fundamental_multiples_columns(stock.split("_")[0],
                                                                stock.split("_")[1],
                                                                freq=freq, columnas=nombre_serie)

        data= dabase_functions.obtener_multiples_series("fundamental", freq, *([stock]+nombre_serie))

    if fecha_corte is not None:
        data=data.loc[data.index>dt.datetime.strptime(fecha_corte, "%Y-%m-%d")]
    if isinstance(nombre_serie,str):
        plots_iniciales.plot_serie_temporal(data, stock+": "+nombre_serie, nombre_serie, num_plots=4, archivo=None, kind=None)
    elif isinstance(nombre_serie,list):
        print(data.isna().sum(),data.shape)
        plots_iniciales.plot_daraframe_temporal(data, stock, num_plots=4, archivo=None, kind=None)
