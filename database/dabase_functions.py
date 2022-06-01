# os.chdir("../")
from config import load_config
from database import bd_handler
from utils import work_dataframes

config = load_config.config()
import pandas as pd


def filter_by_description(*palabras_clave):
    bd = bd_handler.bd_handler()
    cadena_claves = " description  like %s or" * len(palabras_clave)
    cadena_claves = cadena_claves[:-2]
    valores = ["%" + e + "%" for e in palabras_clave]

    consulta = "select exchange,stock,description from descriptions where" + cadena_claves
    return bd.execute_consult_dataframe(consulta, valores)


def filter_by_broker(broker):
    bd = bd_handler.bd_handler()
    consulta = "select code as stock,exchange,concat(exchange,'_',code) as accion from {}".format(broker)
    data = bd.execute_consult_dataframe(consulta, None)
    data["exchange"] = data["exchange"].transform(lambda x: "US" if x in ["NASDAQ", "NYSE"] else x)
    data["accion"] = data["exchange"] + "_" + data["stock"]
    return data


def filter_by_sector(sector):
    bd = bd_handler.bd_handler()
    consulta = "select stock,exchange,concat(exchange,'_',stock) as accion,sector  from " \
               "sectors where sector like %s"
    return bd.execute_consult_dataframe(consulta, (sector,))


def filter_by_indice(indice, exchange):
    bd = bd_handler.bd_handler()
    if exchange not in config["macro"]["exchange_other_name"].keys():
        consulta = "select code as stock,exchange,concat(exchange,'_',code) as accion  from " \
                   "stocks inner join indices on stocks.code=indices.company where stocks.exchange=%s and indices.indice=%s"
        return bd.execute_consult_dataframe(consulta, (exchange, indice))
    else:
        df_stocks_names = filter_by_indice(indice, config["macro"]["exchange_other_name"][exchange][0])
        df_stocks_names["exchange"] = exchange
        df_stocks_names["accion"] = df_stocks_names["exchange"] + "_" + df_stocks_names["stock"]
        for e_aux in config["macro"]["exchange_other_name"][exchange][1:]:
            df = filter_by_indice(indice, e_aux)
            df["exchange"] = exchange
            df["accion"] = df["exchange"] + "_" + df["stock"]
            df_stocks_names = pd.concat([df_stocks_names, df], axis=0)
            return df_stocks_names


def get_fundamental_multiples_columns(exchange, stock, freq="M", columnas=[]):
    """
    Para obtener mltiples columnas de fundamental de un stock
    :param exchange:
    :param stock:
    :param freq:
    :param columnas: olumnas que se quieren incluir. No incluye adjusted_close por defecto
    :return:
    """
    bd = bd_handler.bd_handler()
    nombres_columnas = ""
    for e in columnas[:-1]:
        nombres_columnas += e + ", "
    nombres_columnas += (columnas[-1])

    data = bd.execute_consult_dataframe("select fecha,{} from {} where stock=%s "
                                        "order by fecha asc".format(nombres_columnas, exchange + "_" + "fundamental"),
                                        (stock,))
    data.index = pd.to_datetime(data.fecha)
    data.drop("fecha", axis=1, inplace=True)

    data = data.resample(freq).last()
    return data


# extrae los resulados de fundamental guardados ya con los precios de un mes mas tarde del report date
def get_fundamental_multiples_columns_report_dates(exchange, stock, freq="M", columnas=[]):
    """
    Para obtener diferentes columnas de la tabla ratios_results de un mismo stock, donde esta guardado el fundamental y el precio de un
    mes siguiente del report_date
    :param exchange:
    :param stock:
    :param freq:
    :param columnas: columnas que se quieren incluir. No incluye adjusted_close por defecto
    :return:
    """
    bd = bd_handler.bd_handler()
    nombres_columnas = ""
    for e in columnas[:-1]:
        nombres_columnas += e + ", "
    nombres_columnas += (columnas[-1])

    data = bd.execute_consult_dataframe("select fecha,{} from ratios_results  where exchange=%s and stock=%s "
                                        "order by fecha asc".format(nombres_columnas), (exchange, stock))
    data.index = pd.to_datetime(data.fecha)
    data.drop("fecha", axis=1, inplace=True)

    data = data.resample(freq).last()
    return data


def get_prize_or_fundamenal(exchange, stock, freq="D", type="precios", columna="adjusted_close", col_name=None):
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
    bd = bd_handler.bd_handler()
    if col_name is None:
        data = bd.execute_consult_dataframe("select fecha,{} from {} where stock=%s "
                                            "order by fecha asc".format(columna, exchange + "_" + type), (stock,))
    else:
        data = bd.execute_consult_dataframe("select fecha,{} as {} from {} where stock=%s "
                                            "order by fecha asc".format(columna, col_name, exchange + "_" + type),
                                            (stock,))

    data.index = pd.to_datetime(data.fecha)
    data.drop("fecha", axis=1, inplace=True)

    data = data.resample(freq).last()
    return data


def get_series_activos_diferentes_de_acciones(country, event, tipo: str, freq='M', col_name=None):
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
        bd = bd_handler.bd_handler()
        serie = bd.execute_consult_dataframe(
            "select Date as fecha,close from preciosindices where indice=%s order by fecha asc", (event,))


    elif tipo == "macro":
        bd = bd_handler.bd_handler(config["bd"]["database_calendarios"])
        serie = bd.execute_consult_dataframe(
            "select date as fecha,actual from calendarioEsperado where zone=%s and event like %s"
            " order by fecha asc", (country, event))
    elif tipo == "forex":
        bd = bd_handler.bd_handler(config["bd"]["database_forex"])
        serie = bd.execute_consult_dataframe(
            "select time as fecha,close from {}"
            " order by fecha asc".format(event), None)
    elif tipo == "commodities":
        bd = bd_handler.bd_handler(config["bd"]["database_commodities"])
        serie = bd.execute_consult_dataframe(
            "select  fecha,close from commodities"
            " where name=%s order by fecha desc", (event,))
    else:
        bd = bd_handler.bd_handler(config["bd"]["database_otros_activos"])
        serie = bd.execute_consult_dataframe(
            "select  fecha,close from {}"
            " where name=%s order by fecha desc".format(tipo), (event,))


    serie.index = pd.to_datetime(serie.fecha)
    serie = serie.drop("fecha", axis=1)
    if col_name is not None:
        serie = serie.rename({serie.columns[0]: col_name}, axis='columns')
    serie = serie.resample(freq).last()
    return serie


def get_multiple_macro_data(pais, shift=0):
    """

    :param pais: pais de los eventos
    :param shift:  desplazamiento (eventos en la fecha de un mes soguiente a la de su publicacion por ejemplo)
    :return:
    """
    series_indices = []
    for i in config["macro"]["country_indices"][pais]:
        print(i)
        series_indices.append(get_series_activos_diferentes_de_acciones(pais, i, "index", "M", i))

    forex = config["macro"]["country_forex"][pais][0]
    serie2 = get_series_activos_diferentes_de_acciones(pais, forex, "forex", "M", forex)
    eventos_macro = config["macro"]["eventos_fundamentales"][pais]
    array_series = [serie2] + series_indices
    for evento in eventos_macro:
        serie = get_series_activos_diferentes_de_acciones(pais, evento, "macro", "M", evento.replace("%", "").replace("()", "").strip())
        array_series.append(serie)
    data = work_dataframes.merge(array_series)
    data = data.shift(shift)
    return data


# funcion para cuando queramos obtener un dataframe con un conjunto de datos para analizar su evolucion temporal conjunta
# el analisis macro va aparte en el modulo macro
def obtener_multiples_series(tipo, freq, *array):
    """
    Para obtener datframe con conjunto de activos de un tipo.
    :param tipo: tipo de series: index, forex, precios (precios de diferentes stocks), fundamental (fundamental y precios para un simbolo),


    :param array:
    """

    series = []
    data = None
    if tipo == "index":
        for nombre in array:
            print(nombre)
            serie = get_series_activos_diferentes_de_acciones("", nombre, "index", freq=freq, col_name=nombre)
            series.append(serie)
    elif tipo == "forex":
        for nombre in array:
            serie = get_series_activos_diferentes_de_acciones("", nombre, "forex", freq=freq, col_name=nombre)
            series.append(serie)


    elif tipo == "precios":
        for nombre in array:
            exchange = nombre.split("_")[0]
            stock = nombre.split("_")[1]
            serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                            col_name=stock)
            series.append(serie)


    elif tipo == "fundamental":
        stock = array[0]
        exchange = stock.split("_")[0]
        stock = stock.split("_")[1]
        serie = get_fundamental_multiples_columns(exchange, stock, freq=freq, columnas=array[1:])
        series.append(serie)
        serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                        col_name=stock)
        series.append(serie)

    data = work_dataframes.merge(series)
    return data


# cuando se especifican las series con dos arrays: por ejemplo el primero para los nombres de las acciones y el segundo
# para las columnas que queremos obtener
# incluye por defecto adjusted_close
def obtener_fundamenal_varios_stocks_multiples_columns(freq, array1=None, array2=None, diccionario=None):
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
            serie = get_fundamental_multiples_columns(exchange, stock, freq=freq, columnas=array2)
            serie.columns = [stock + "_" + e for e in serie.columns]
            series.append(serie)
            serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                            col_name=stock)

            series.append(serie)
    elif diccionario is not None:
        for stock, columnas in diccionario.items():
            exchange = stock.split("_")[0]
            stock = stock.split("_")[1]
            serie = get_fundamental_multiples_columns(exchange, stock, freq=freq, columnas=columnas)
            series.append(serie)
            serie = get_prize_or_fundamenal(exchange, stock, freq=freq, type="precios", columna="adjusted_close",
                                            col_name=stock)
            series.append(serie)

    data = work_dataframes.merge(series)
    return data


# no incluye por defecto adjusted_close
def obtener_fundamenal_varios_stocks_multiples_columns_with_report_dates(freq, array1=None, array2=None,
                                                                         diccionario=None):
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
            serie = get_fundamental_multiples_columns_report_dates(exchange, stock, freq=freq, columnas=array2)
            serie.columns = [stock + "_" + e for e in serie.columns]
            series.append(serie)
    elif diccionario is not None:
        for stock, columnas in diccionario.items():
            exchange = stock.split("_")[0]
            stock = stock.split("_")[1]
            serie = get_fundamental_multiples_columns_report_dates(exchange, stock, freq=freq, columnas=columnas)
            series.append(serie)

    data = work_dataframes.merge(series)
    return data


if __name__ == "__main__":
    data = get_series_activos_diferentes_de_acciones("", config["macro"]["materias_primas"][0], "commodities", freq='B')
    data = get_series_activos_diferentes_de_acciones("", "nasdaq", "index", freq='B', col_name='nasdaq')
    data = get_series_activos_diferentes_de_acciones("united kingdom", config["macro"]["eventos_fundamentales"]["united kingdom"][6], "macro",
                                                     freq='M')

    data = filter_by_broker("admiralmarkets")

    print(data.tail())
    data = filter_by_sector("%tec%")
    print(data.tail())
    data = filter_by_indice("ibex35", "mc")
    print(data.tail())
    data = obtener_multiples_series("indices", "M", *["SP500", "NASDAQ", "ibex35"])
    data = obtener_multiples_series("forex", "M", *["EUR_USD", "GBP_USD"])
    data = obtener_multiples_series("precios", "M", *["US_AAPL", "US_AMZN", "LSE_NCC"])
    data = obtener_multiples_series("fundamental", "Q", *["US_AAPL", "netIncome"])
    data = obtener_fundamenal_varios_stocks_multiples_columns("Q", ["US_AAPL", "US_AMZN"], ["netIncome", "totalAssets"])
    data = get_fundamental_multiples_columns_report_dates("US", "AAPL", freq="M",
                                                          columnas=["netIncome", "adjusted_close"])
    data = obtener_fundamenal_varios_stocks_multiples_columns_with_report_dates("M", array1=["US_AMZN", "LSE_0LF0"],
                                                                                array2=["adjusted_close", "netincome"],
                                                                                diccionario=None)
    print(data.tail())
