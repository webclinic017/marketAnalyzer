
import os
#os.chdir("../../")
import numpy as np
import pandas as pd
from utils.get_new_data.stocks_data_EOD_implementation.get_fundamentals import get_fundamentals
from utils.get_new_data.stocks_data_EOD_implementation.get_prizes import  get_eod_prizes
from utils.database import  bd_handler
PRINT = False
pd.options.mode.chained_assignment = None
import datetime as dt
from datetime import timedelta
from  typing import Type, Callable
def update_bd(data, exchange, stock, type,bd):

    data["exchange"] = exchange
    data["stock"] = stock
    data.index.names = ["fecha"]
    bd.bulk_insert(data,exchange +"_" + type)

def check_exchange(exchange: str or list ,type:str, function,bd ):
    """

    :param exchange:
    :param type:
    :param function:
    :type function:  Callable
    """

    codigo=bd.execute_query("SHOW TABLES LIKE %s;",(exchange +"_" + type,))
    if codigo is None:
        data = function(
            "AMZN.US")
        if "currency_symbol" in data.columns:
            data.drop(labels="currency_symbol", inplace=True, axis=1)
        cols = " double,".join([str(i) for i in data.columns.tolist()])
        cols = "fecha date," + cols
        cols = cols + \
               " double,stock varchar(100),exchange varchar(100), PRIMARY KEY (stock,exchange,fecha));"

        sql = "CREATE TABLE " + exchange + "_"+type + " (" + cols
        bd.execute(sql)
        # the connection is not autocommitted by default, so we must commit to save our # changes




def save_stocks_data(stock, exchange, tipo,bd):
    if tipo=="precios":
        api_function=get_eod_prizes
    elif tipo=="fundamental":
        api_function =get_fundamentals
    else:
        return

    sql = "select max(fecha) from  {} where stock=%s order by fecha desc limit 1".format(
        exchange+"_"+tipo)
    date = bd.execute_query(sql, (stock,))[0][0]
    last_date=dt.date.today()
    if last_date.weekday()==6:
        last_date=last_date-dt.timedelta(days=2)
    else:
        last_date = last_date - dt.timedelta(days=1)

    data=None
    if date is None:

        data = api_function(
            stock + "." + exchange, to_date=last_date)

    elif last_date>date:
        data = api_function(
            stock + "." + exchange, from_date=date+timedelta(days=1), to_date=last_date)
    if data is not None and len(data)>0:
        data.index = pd.to_datetime(data.index)
        update_bd(data, exchange, stock,tipo,bd)


if __name__ == "__main__":
    save_stocks_data("AAPL","US","precios")
    save_stocks_data("AAPL", "US", "fundamental")
