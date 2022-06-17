import logging,logging.config
from logging import getLogger
from utils.get_new_data import stocks
from requests.exceptions import HTTPError
import datetime as dt
import pandas as pd
import configparser
from utils.get_new_data.stocks_data_EOD_implementation.get_prizes import  get_eod_prizes
from utils.get_new_data.stocks_data_EOD_implementation.get_fundamentals import  get_fundamentals
from utils.get_new_data import  highlights
logger = logging.getLogger('getting_data')
config = configparser.ConfigParser()
config.read('config/config_key.properties')
directorio = config.get('ARCHIVOS', 'archivos_h5s_precios')

def __update_prizes_h5s(exchange,stock,bd):
    filename = directorio + "precios_mensual_" + exchange
    h5s = pd.HDFStore(filename + ".h5s", "r")
    temp = h5s["data"]
    h5s.close()
    temp=temp.loc[temp.index.get_level_values(1)!=stock]
    data = bd.execute_query_dataframe(
        "select fecha,stock,Adjusted_close from {}_precios where stock=%s order by fecha asc".format(exchange), (stock,))
    if data is not None:
        data["fecha"] = pd.to_datetime(data["fecha"])
        data = data.groupby([data["stock"], data["fecha"].dt.year, data["fecha"].dt.month], as_index=False).last()
        data["fecha"] = data["fecha"].transform(lambda x: x.replace(day=1))
        data.set_index(["fecha", "stock"], inplace=True)
        data = data[~data.index.duplicated(keep='last')]
        data = pd.concat([temp, data], axis=0)
    else:
        data=temp
    data = data[~data.index.duplicated(keep='last')]
    data.sort_index(inplace=True)
    h5s = pd.HDFStore(filename + ".h5s", "w")
    h5s["data"] = data
    h5s.close()






def reinit_stock(values, bd):
    """We need to update all the tables where there is information about the number of the stocks or the prize of the stock.split("_")[
    We neet to check with tables havent been updated yet.


    :param values: pandas series with split data
    :param bd: bd to update the data
    """
    sql="select * from splits where exchange=%s and stock=%s and split_date>=%s and    updatedOtherTables=1"
    updates=bd.execute_query_dataframe(sql, (values.exchange, values.stock, values.split_date))
    if updates is None:
        stocks.check_exchange(values.exchange, "precios", get_eod_prizes, bd)
        stocks.check_exchange(values.exchange, "fundamental", get_fundamentals, bd)

        sql1 = "delete  from {}_precios where stock=%s".format(values.exchange)
        bd.execute(sql1,(values.stock,))
        sql2 = "delete  from {}_fundamental where stock=%s".format(values.exchange)
        bd.execute(sql2,(values.stock,))
        sql3 = "delete  from ratios_results  where exchange=%s and stock=%s"
        bd.execute(sql3,(values.exchange,values.stock,))
        try:
            stocks.save_stocks_data(values.stock, values.exchange, "precios", bd)
            stocks.save_stocks_data(values.stock, values.exchange, "fundamental", bd)
            __update_prizes_h5s(values.exchange, values.stock, bd)


            sql="select * from splits where exchange=%s and stock=%s and split_date=%s"
            updates = bd.execute_query_dataframe(sql, (values.exchange, values.stock, values.split_date))


            if updates is not None and len(updates)>0:
                sql = "update  splits set  updatedOtherTables=1 where exchange=%s and stock=%s and split_date=%s"
                bd.execute(sql, (values.exchange, values.stock, values.split_date))
            else:
                columnas=",".join(list(values.index))
                spaces=",".join(["%s" for _ in  range(len(values.index))])
                sql = "insert into  splits ({},updatedOtherTables) values({},%s)".format(columnas,spaces)
                bd.execute(sql, (list(values.values)+[1]))
            logger.info("main_splits: split values {}".format(values))
        except HTTPError as e:
            logger.error("reinit_stock: {}".format(e))


def update_one_stock_in_all_tables(exchange,stock,bd):
    stocks.save_stocks_data(stock,exchange, "precios", bd)
    stocks.save_stocks_data(stock, exchange, "fundamental", bd)
    highlights.update_highlights([stock,exchange], dt.datetime.today().date(), bd)











