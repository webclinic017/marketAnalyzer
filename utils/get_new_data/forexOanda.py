import configparser
import datetime as dt
from datetime import timedelta
import mysql.connector
import pandas as pd
import sqlalchemy
import v20
import os
os.chdir("../../")
config = configparser.ConfigParser()
config.read('config/config.properties')
HOST = config.get('DatabaseForexSection', 'database_host')
USER = config.get('DatabaseForexSection', 'database_user')
PASSWORD = config.get('DatabaseForexSection', 'database_password')
PORT = config.get('DatabaseForexSection', 'database_port')
DATABASE1 = config.get('DatabaseForexSection', 'database_name')
database_username = USER
database_password = PASSWORD
database_ip = HOST + ":" + PORT
database_name = DATABASE1
hostname = config.get('OANDA_API_Section', "hostname")
port = config.get('OANDA_API_Section', "port")
token = config.get('OANDA_API_Section', "token")
cuenta = config.get('OANDA_API_Section', "cuenta")
ssl = True if config.get('OANDA_API_Section', "ssl") == "True" else False
datetime_format = config.get('OANDA_API_Section', "datetime_format")
periodicidad = config.get('OANDA_API_Section', "periodicidad")
engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
    database_username, database_password, database_ip, database_name), pool_recycle=3600, pool_size=5).connect()


def init():
    pares = ["EUR_USD", "EUR_GBP", "EUR_JPY", "EUR_CHF", "EUR_CAD", "EUR_AUD", "GBP_USD", "GBP_AUD",
             "GBP_CAD", "AUD_CAD", "CAD_CHF", "CAD_JPY", "AUD_CHF", "AUD_JPY", "CHF_JPY", "USD_CHF"]
    ctx = v20.Context(
        hostname,
        port,
        ssl,
        application="sample_code",
        token=token,
        datetime_format=datetime_format
    )

    kwargs = {}
    kwargs["granularity"] = periodicidad

    kwargs["price"] = "B"
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        port=PORT,
        password=PASSWORD, database="forex"
    )
    mycursor = mydb.cursor()
    return ctx, kwargs, mycursor, mydb, pares


ctx, kwargs, mycursor, mydb, pares = init()


def getDataFromApi():
    for par in pares:

        sql = "show tables;"
        mycursor.execute(sql)
        tablas = [e[0] for e in mycursor.fetchall()]
        print(tablas)
        if par in tablas:
            sql = "select time from  {} order by time desc limit 1".format(par)
            mycursor.execute(sql)
            fecha = pd.DataFrame(mycursor.fetchone()).iloc[0, 0]
            fecha = dt.datetime(fecha.year, fecha.month, fecha.day)
            mydb.commit()
        else:
            sql = "create table {}(time date,close double,open double,low double,high double,volume double,spread double);".format(
                par)
            mycursor.execute(sql)
            mydb.commit()
            fecha = dt.datetime(2005, 1, 1)

        symbol = par
        kwargs["count"] = 5000

        today = dt.datetime.now() - timedelta(days=1)
        desde = fecha
        desde1 = desde + timedelta(days=1)
        print("Fecha inicial %s para simbolo %s" % (desde, symbol))
        print("Fecha final  %s para simbolo %s" % (today, symbol))
        print("------------------------------")
        desde = dt.datetime.timestamp(desde)
        kwargs["fromTime"] = desde
        today = dt.datetime.timestamp(today)
        prize_df = pd.DataFrame(
            columns=["time", "close", "open", "low", "high", "volume", "spread"])
        prize_df.set_index("time", inplace=True, drop=True)
        desde2 = desde
        while (desde < today):
            try:
                kwargs["price"] = "B"
                response = ctx.instrument.candles(symbol, **kwargs)

                kwargs["price"] = "A"
                response3 = ctx.instrument.candles(symbol, **kwargs)
                z = response.get("candles", 200)
                z3 = response3.get("candles", 200)

                j = 0
                for i in z:

                    if dt.datetime.fromtimestamp(float(i.time)).replace(hour=0, minute=0, second=0,
                                                                        microsecond=0) not in pd.to_datetime(
                        prize_df.index).tolist():
                        prize_df.loc[dt.datetime.fromtimestamp(float(i.time))] = [
                            i.bid.c, i.bid.o, i.bid.l, i.bid.h, i.volume, z3[j].ask.c - z[j].bid.c]
                        j += 1
                desde = float(z[len(z) - 1].time) + 60 * 60

                kwargs["fromTime"] = desde
                # print("TIEMPO DE PARADA %s" % dt.datetime.fromtimestamp(desde))
                prize_df = prize_df.loc[prize_df.index >= desde1]
                prize_df.index = prize_df.index.normalize()
                prize_df.to_sql(par, engine, if_exists="append", chunksize=1000)
                # print(prize_df.head())
                # print(prize_df.tail())
                if desde == desde2:
                    break
                desde2 = desde
            except Exception as e:
                print(e)


if __name__ == "__main__":
    getDataFromApi()
