#os.chdir("../../")
from config import load_config
from utils.database import bd_handler
from utils.dataframes import work_dataframes

config = load_config.config()
import pandas as pd


def filter_by_description(*palabras_clave,bd):

    cadena_claves = " description  like %s or" * len(palabras_clave)
    cadena_claves = cadena_claves[:-2]
    valores = ["%" + e + "%" for e in palabras_clave]

    consulta = "select exchange,stock,description from descriptions where" + cadena_claves
    data= bd.execute_query_dataframe(consulta, valores)
    data["accion"] = data["exchange"] + "_" + data["stock"]
    return data


def filter_by_broker(broker,bd):

    consulta = "select code as stock,exchange,concat(exchange,'_',code) as accion from {}".format(broker)
    data = bd.execute_query_dataframe(consulta, None)
    return data


def filter_by_sector(sector,bd):

    consulta = "select stock,exchange,concat(exchange,'_',stock) as accion,sector  from " \
               "sectors where sector like %s"
    data=bd.execute_query_dataframe(consulta, (sector,))
    return  data


def filter_by_indice(indice, exchange,bd):


    consulta = "select code as stock,exchange,concat(exchange,'_',code) as accion  from " \
                   "stocks inner join indices on stocks.code=indices.company where stocks.exchange=%s and indices.indice=%s"
    data= bd.execute_query_dataframe(consulta, (exchange, indice))
    return data



def get_fundamental_multiples_columns(exchange, stock, freq="M", columnas=[],bd=None):
    """
    Para obtener mltiples columnas de fundamental de un stock
    :param exchange:
    :param stock:
    :param freq:
    :param columnas: olumnas que se quieren incluir. No incluye adjusted_close por defecto
    :return:
    """

    nombres_columnas = ""
    for e in columnas[:-1]:
        nombres_columnas += e + ", "
    nombres_columnas += (columnas[-1])

    data = bd.execute_query_dataframe("select fecha,{} from {} where stock=%s "
                                        "order by fecha asc".format(nombres_columnas, exchange + "_" + "fundamental"),
                                      (stock,))
    data.index = pd.to_datetime(data.fecha)
    data.drop("fecha", axis=1, inplace=True)

    data = data.resample(freq).last()
    return data


# extrae los resulados de fundamental guardados ya con los precios de un mes mas tarde del report date
def get_fundamental_multiples_columns_report_dates(exchange, stock, freq="M", columnas=[],bd=None):
    """
    Para obtener diferentes columnas de la tabla ratios_results de un mismo stock, donde esta guardado el fundamental y el precio de un
    mes siguiente del report_date
    :param exchange:
    :param stock:
    :param freq:
    :param columnas: columnas que se quieren incluir. No incluye adjusted_close por defecto
    :return:
    """

    nombres_columnas = ""
    for e in columnas[:-1]:
        nombres_columnas += e + ", "
    nombres_columnas += (columnas[-1])

    data = bd.execute_query_dataframe("select fecha,{} from ratios_results  where exchange=%s and stock=%s "
                                        "order by fecha asc".format(nombres_columnas), (exchange, stock))
    data.index = pd.to_datetime(data.fecha)
    data.drop("fecha", axis=1, inplace=True)

    data = data.resample(freq).last()
    return data


def get_prize_or_fundamenal(exchange, stock, freq="D", type="precios", columna="adjusted_close", col_name=None,bd=None):
    """
    Para obtener precios o fundamental de un stock.
    :param exchange:
    :param stock:
    :param freq: frecuencia: D,B,M,Q,QS
    :param type: precios o fundamental
    :param columna: nombre de la columna de la tabla
    :param col_name: nombre que se le quiere poner a la columna
    :return: dataframe con una columna
    """

    if col_name is None:
        data = bd.execute_query_dataframe("select fecha,{} from {} where stock=%s "
                                            "order by fecha asc".format(columna, exchange + "_" + type), (stock,))
    else:
        data = bd.execute_query_dataframe("select fecha,{} as {} from {} where stock=%s "
                                            "order by fecha asc".format(columna, col_name, exchange + "_" + type),
                                          (stock,))

    data.index = pd.to_datetime(data.fecha)
    data.drop("fecha", axis=1, inplace=True)

    data = data.resample(freq).last()
    return data


def get_series_activos_diferentes_de_acciones(country, event, tipo: str, freq='M', col_name=None,bd=None):
    """
    Para obtener dataframe de activos que no sean acciones. Series de una sola columna.
    :param country: pais cuando la serie este asociada a un pais
    :param event: columna de la tabla que se quiere coger
    :param tipo: index, macro, forex, commodities,
    :param freq: frecuencia: D,B, M, Q, QS
    :param col_name: nombre que se le quiere asociar a la columna de la tabla
    :return:
    """
    if tipo == "index":

        serie = bd.execute_query_dataframe(
            "select Date as fecha,close from indices where name=%s order by fecha asc", (event,))


    elif tipo == "macro":

        serie = bd.execute_query_dataframe(
            "select date as fecha,actual from calendar where zone=%s and event like %s"
            " order by fecha asc", (country, event))
    elif tipo == "forex":

        serie = bd.execute_query_dataframe(
            "select date as fecha,close from forex where name=%s"
            " order by fecha asc", (event,))
    elif tipo == "commodities":

        serie = bd.execute_query_dataframe(
            "select date as  fecha ,close from commodities"
            " where name=%s order by fecha asc", (event,))
    else:

        serie = bd.execute_query_dataframe(
            "select  date as fecha,close from {}"
            " where name=%s order by fecha asc".format(tipo), (event,))


    serie.index = pd.to_datetime(serie.fecha)
    serie = serie.drop("fecha", axis=1)
    if col_name is not None:
        serie = serie.rename({serie.columns[0]: col_name}, axis='columns')
    serie = serie.resample(freq).last()
    return serie


def get_multiple_macro_data(pais, shift=0,bd=None):
    """

    :param pais: pais de los eventos
    :param shift:  desplazamiento (eventos en la fecha de un mes soguiente a la de su publicacion por ejemplo)
    :return:
    """
    series_indices = []
    for i in config["macro"]["country_indices"][pais]:
        print(i)
        series_indices.append(get_series_activos_diferentes_de_acciones(pais, i, "index", "M", i,bd))

    forex = config["macro"]["country_forex"][pais][0]
    serie2 = get_series_activos_diferentes_de_acciones(pais, forex, "forex", "M", forex,bd)
    eventos_macro = config["macro"]["eventos_fundamentales"][pais]
    array_series = [serie2] + series_indices
    for evento in eventos_macro:
        serie = get_series_activos_diferentes_de_acciones(pais, evento, "macro", "M", evento.replace("%", "").replace("()", "").strip(),bd)
        array_series.append(serie)
    data = work_dataframes.merge(array_series)
    data = data.shift(shift)
    return data


# funcion para cuando queramos obtener un dataframe con un conjunto de datos para analizar su evolucion temporal conjunta
# el analisis macro va aparte en el modulo macro
def obtener_multiples_series(tipo, freq, *array,bd=None):
    """
    Para obtener datframe con conjunto de activos de un tipo.
    :param tipo: tipo de series: index, forex, precios (precios de diferentes stocks), fundamental (fundamental y precios para un simbolo),


    :param array:
    """

    series = []

    if tipo == "index":
        for nombre in array:
            print(nombre)
            serie = get_series_activos_diferentes_de_acciones("", nombre, "index", freq=freq, col_name=nombre,bd=bd)
            series.append(serie)
    elif tipo == "forex":
        for nombre in array:
            serie = get_series_activos_diferentes_de_acciones("", nombre, "forex", freq=freq, col_name=nombre,bd=bd)
            series.append(serie)


    elif tipo == "precios":
        for nombre in array:
            exchange = nombre.split("_")[0]
            stock = nombre.split("_")[1]
            serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                            col_name=exchange+"_"+stock,bd=bd)
            series.append(serie)


    elif tipo == "fundamental":
        stock = array[0]
        exchange = stock.split("_")[0]
        stock = stock.split("_")[1]
        serie = get_fundamental_multiples_columns(exchange, stock, freq=freq, columnas=array[1:],bd=bd)
        series.append(serie)
        serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                        col_name=exchange+"_"+stock,bd=bd)
        series.append(serie)

    data = work_dataframes.merge(series)
    return data


# cuando se especifican las series con dos arrays: por ejemplo el primero para los nombres de las acciones y el segundo
# para las columnas que queremos obtener
# incluye por defecto adjusted_close
def obtener_fundamenal_varios_stocks_multiples_columns(freq, array1=None, array2=None, diccionario=None,bd=None):
    """
    Para obtener datos de fundamental de diferentes acciones. Añade para todas las accioens el precio de esa accion
    :param freq: frecuencia para el resample
    :param array1: array con los nombres de los stocks (exchange_stock)
    :param array2:  array con los nombres de las columnas
    :param  diccionario:  diccionario con las claves los nombres de los stocks y los valores el array de columnas para
        cada stock
    """
    series = []
    if array1 is not None:
        for stock in array1:
            exchange = stock.split("_")[0]
            stock = stock.split("_")[1]
            serie = get_fundamental_multiples_columns(exchange, stock, freq=freq, columnas=array2,bd=bd)
            serie.columns = [exchange+ "_"+stock+ "_" + e for e in serie.columns]
            series.append(serie)
            serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                            col_name=exchange+"_"+stock,bd=bd)

            series.append(serie)
    elif diccionario is not None:
        for stock, columnas in diccionario.items():
            exchange = stock.split("_")[0]
            stock = stock.split("_")[1]
            serie = get_fundamental_multiples_columns(exchange, stock, freq=freq, columnas=columnas,bd=bd)
            serie.columns = [exchange + "_" + stock + "_" + e for e in serie.columns]
            series.append(serie)
            serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                            col_name=exchange+"_"+stock,bd=bd)
            series.append(serie)

    data = work_dataframes.merge(series)
    return data


# no incluye por defecto adjusted_close
def obtener_fundamenal_varios_stocks_multiples_columns_with_report_dates(freq, array1=None, array2=None,
                                                                         diccionario=None,bd=None):
    """
    Para obtener datos de fundamental de diferentes acciones. No añade el precio por defecto, hay que añadir la columna
    adjusted_close. Igual que antes pero usa la tabla con los precios asociados mes sigguiente al report date en vez de
    a la fecha de los resltados
    :param freq: frecuencia para el resample
    :param array1: array con los nombres de los stocks (exchange_stock)
    :param array2:  array con los nombres de las columnas
    """
    series = []
    if array1 is not None:
        for stock in array1:
            exchange = stock.split("_")[0]
            stock = stock.split("_")[1]
            serie = get_fundamental_multiples_columns_report_dates(exchange, stock, freq=freq, columnas=array2,bd=bd)
            serie.columns = [exchange+"_"+stock + "_" + e for e in serie.columns]
            series.append(serie)
    elif diccionario is not None:
        for stock, columnas in diccionario.items():
            exchange = stock.split("_")[0]
            stock = stock.split("_")[1]
            serie = get_fundamental_multiples_columns_report_dates(exchange, stock, freq=freq, columnas=columnas,bd=bd)
            serie.columns = [exchange + "_" + stock + "_" + e for e in serie.columns]
            series.append(serie)

    data = work_dataframes.merge(series)
    return data


if __name__ == "__main__":

    bd1=bd_handler.bd_handler("stocks")
    bd2=bd_handler.bd_handler("market_data")
    data = get_series_activos_diferentes_de_acciones("", config["macro"]["materias_primas"][0], "commodities", freq='B',bd=bd2)
    data = get_series_activos_diferentes_de_acciones("", "nasdaq", "index", freq='B', col_name='nasdaq',bd=bd2)
    data = get_series_activos_diferentes_de_acciones("united kingdom", config["macro"]["eventos_fundamentales"]["united kingdom"][6], "macro",
                                                     freq='M',bd=bd2)

    data = filter_by_broker("admiralmarkets",bd=bd1)

    print(data.tail())
    data = filter_by_sector("%tec%",bd=bd1)
    print(data.tail())
    data = filter_by_indice("ibex 35", "mc",bd=bd1)
    print(data.tail())
    data = obtener_multiples_series("index", "M", *["S&P 500", "NASDAQ", "ibex 35"],bd=bd2)
    print(data.tail())
    data = obtener_multiples_series("forex", "M", *["EUR/USD", "GBP/USD"],bd=bd2)
    print(data.tail())
    data = obtener_multiples_series("precios", "M", *["US_AAPL", "US_AMZN", "LSE_NCC"],bd=bd1)
    print(data.tail())
    data = obtener_multiples_series("fundamental", "Q", *["US_AAPL", "netIncome"],bd=bd1)
    print(data.tail())
    data = obtener_fundamenal_varios_stocks_multiples_columns("Q", ["US_AAPL", "US_AMZN"], ["netIncome", "totalAssets"],bd=bd1)
    data = get_fundamental_multiples_columns_report_dates("US", "AAPL", freq="M",
                                                          columnas=["netIncome", "adjusted_close"],bd=bd1)
    data = obtener_fundamenal_varios_stocks_multiples_columns_with_report_dates("M", array1=["US_AMZN", "LSE_0LF0"],
                                                                                array2=["adjusted_close", "netincome"],
                                                                                diccionario=None,bd=bd1)
    print(data.tail())
