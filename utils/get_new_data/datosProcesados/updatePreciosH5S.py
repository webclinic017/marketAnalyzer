



import os
os.chdir("../../../")
from database import bd_handler
bd = bd_handler.bd_handler()
import pandas as pd
import configparser
import time
import datetime as dt
from datetime import timedelta


if __name__=="__main__":
    config = configparser.ConfigParser()
    config.read('config/config.properties')
    directorio = config.get('ARCHIVOS', 'archivos_h5s_precios')
    sql = "show tables"
    tablas = bd.execute_consult(sql)
    tablas = [tabla[0] for tabla in tablas]
    fechaI = (dt.datetime.today() - timedelta(days=65)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    TABLAS = [tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "precios")]
    for tabla in TABLAS:

        exchange = tabla.split("_")[0]
        if exchange != "US":
            existe = True
            try:
                filename = directorio + "precios_mensual_" + exchange
                h5s = pd.HDFStore(filename + ".h5s", "r")
                temp = h5s["data"]
                h5s.close()

            except Exception as e:
                print(e)

                existe = False

            print(exchange)
            try:
                tiempo1 = time.time()
                if existe:
                    data = bd.execute_consult_dataframe(
                        "select fecha,stock,Adjusted_close from {}_precios  where fecha>%s order by fecha asc".format(
                            exchange), (fechaI,))
                else:
                    data = bd.execute_consult_dataframe(
                        "select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange), None)
                # data=engine.execute("select fecha,stock,Adjusted_close from {}_precios order by fecha asc".format(exchange))
                data["fecha"] = pd.to_datetime(data["fecha"])
                data = data.groupby([data["stock"], data["fecha"].dt.year, data["fecha"].dt.month], as_index=False).last()
                data["fecha"] = data["fecha"].transform(lambda x: x.replace(day=1))
                data.set_index(["fecha", "stock"], inplace=True)
                data = data[~data.index.duplicated(keep='last')]


                print(len(data))

                if existe:
                    temp = temp.loc[temp.index.get_level_values(0) <= fechaI]
                    print(temp.tail())
                    print(data.head())
                    data = pd.concat([temp, data], axis=0)
                    data = data[~data.index.duplicated(keep='last')]
                    data.sort_index(inplace=True)


                print("------------------------")
                filename = directorio + "precios_mensual_" + exchange

                h5s = pd.HDFStore(filename + ".h5s", "w")
                h5s["data"] = data
                h5s.close()
            except Exception as e:
                print(e)


