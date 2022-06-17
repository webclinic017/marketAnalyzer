
from utils.database import database_functions
from plots import plots_iniciales
import datetime as dt
def plot_specific_series(nombre_serie,stock,freq,fecha_corte,bd,file_name=None):
    if isinstance(nombre_serie,str) and nombre_serie=="adjusted_close":
        data= database_functions.get_prize_or_fundamenal(stock.split("_")[0],
                                                       stock.split("_")[1],
                                                       freq=freq, type="precios",
                                                       columna="adjusted_close", col_name=None,bd=bd)

    elif isinstance(nombre_serie, str):
        data = database_functions.get_prize_or_fundamenal(stock.split("_")[0],
                                                        stock.split("_")[1],
                                                        freq=freq, type="fundamental",
                                                        columna=nombre_serie, col_name=None,bd=bd)
    elif isinstance(nombre_serie, list):

        data= database_functions.obtener_multiples_series("fundamental", freq, *([stock] + nombre_serie),bd=bd)

    if fecha_corte is not None:
        data=data.loc[data.index>dt.datetime.strptime(fecha_corte, "%Y-%m-%d")]
    if isinstance(nombre_serie,str):
        plots_iniciales.plot_serie_temporal(data, stock+": "+nombre_serie, nombre_serie, num_plots=4, archivo=file_name, kind=None)
    elif isinstance(nombre_serie,list):
        print(data.isna().sum(),data.shape)
        plots_iniciales.plot_daraframe_temporal(data, stock, num_plots=4, archivo=file_name, kind=None)

