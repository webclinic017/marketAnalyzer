from utils.get_new_data.stocks_data_EOD_implementation.get_highLights import getHighLights
import logging, logging.config
from logging import getLogger
from requests.exceptions import HTTPError
import datetime as dt
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
def update_highlights(values:list,fechalim:dt.datetime.date,bd):
    """

    :param values: values[1] must be exchange and values[0] must be stock
    :param fechalim: date from where we update
    :param bd: bd
    """
    pillado = False
    for tabla in ["highlights", "last_highlights"]:
        fecha = bd.execute_query(
            "select fecha from {} where exchange=%s and stock=%s order by fecha desc limit 1".format(tabla),
            (values[1], values[0]))

        if fecha is None or len(fecha) == 0 or fecha[0][0] < fechalim:

            try:

                if pillado == False:
                    datos = getHighLights(values[0] + "." + values[1])
                    pillado = True

                if tabla == "highlights" or fecha is None or len(fecha) == 0:
                    datos["fecha"] = dt.datetime.today()
                    datos["stock"] = values[0]
                    datos["exchange"] = values[1]
                    placeholder = ", ".join(["%s"] * len(datos))
                    execute = "insert into {} ({}) values ({})".format(tabla, ",".join(datos.keys()), placeholder)
                    bd.execute(execute, list(datos.values()))
                    logger.info("main_high_lights: Data inserted to table {}, exchange {}, stock {} ".format(tabla,
                                                                                                             values[
                                                                                                                 1],
                                                                                                             values[
                                                                                                                 0]))

                else:
                    for key in ["stock", "exchange"]:
                        if key in datos.keys():
                            datos.pop(key)
                    placeholder = "set "
                    for e in datos.keys():
                        placeholder += e + "=%s, "
                    placeholder = placeholder[:-2] + " "
                    execute = "update {} {} where stock=%s and exchange=%s ".format(tabla, placeholder)
                    datos["stock"] = values[0]
                    datos["exchange"] = values[1]
                    bd.execute(execute, list(datos.values()))
                    logger.info("highlights: Data updated to table {}, exchange {}, stock {} ".format(tabla,
                                                                                                            values[
                                                                                                                1],
                                                                                                            values[
                                                                                                                0]))






            except HTTPError as e:
                logger.error("highlights: HTTPERROR: table {}, exchange {}, stock {}: ".format(tabla, values[1],
                                                                                                 values[0]) + str(e))
            except Exception as e:
                logger.error(
                "highlights: ERROR: table {}, exchange {}, stock {}: ".format(tabla, values[1], values[0]) + str(
                    e))

                if tabla == "highlights" or fecha is None or len(fecha) == 0:
                    execute = "insert into {} (fecha,stock,exchange) values (%s,%s,%s)".format(tabla)
                else:
                    execute = "update {} set fecha=%s where stock=%s and exchange=%s".format(tabla)

                bd.execute(execute, (dt.date.today(), values[0], values[1]))

                logger.info(
                    "highlights: Incomplete data added to table {}, exchange {}, stock {} ".format(tabla, values[1],
                                                                                                         values[0]))
