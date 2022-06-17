
import os
os.chdir("../../")
from utils.get_new_data.stocks_data_EOD_implementation.get_fundamentals import get_fundamentals
import stocks
import datetime as dt
import configparser
import pandas as pd
from utils.database import bd_handler
from datetime import timedelta
import logging, logging.config
from logging import getLogger
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
from requests.exceptions import HTTPError
if __name__ == "__main__":

    bd=bd_handler.bd_handler("stocks")

    query = "   select stocks.exchange as exchange, stocks.code  as stock from stocks " \
            "       inner join degiro on stocks.code=degiro.code and stocks.exchange=degiro.exchange " \
            "union " \
            "   select stocks.exchange as exchange, stocks.code   as stock from stocks " \
            "       inner join admiralmarkets " \
            "       on stocks.code=admiralmarkets.code and stocks.exchange=admiralmarkets.exchange " \
            "union " \
            "   select code as exchange,company as stock  from exchanges " \
            "       inner join market_data.indice_country on indice_country.country=exchanges.country " \
            "       inner join indices on indices.indice=indice_country.indice;"
    stock_list_short = bd.execute_query_dataframe(query)
    stock_list_short["action"]=  stock_list_short.exchange+"_"+stock_list_short.stock
    sql="show tables"
    tablas=bd.execute_query(sql)
    tablas=[tabla[0] for tabla in tablas]
    
    
    tablas=[tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "fundamental")]
    


    for tabla in tablas:


            exchange=tabla.split("_")[0]
            sql = " select fund.stock as stock,fecha,date,exchange from ( \
                        select stock,max(fecha) as fecha from {} group by stock) as fund \
                            inner join \
                        (select stock,max(date) as date,exchange from calendarioResultados \
                                     where report_date<%s group by stock,exchange) as c \
                            on  fund.stock=c.stock where c.exchange=%s and date>fecha;".format(tabla)
            stocks_list=bd.execute_query_dataframe(sql,(dt.datetime.today(),exchange))

            if  stocks_list is not None and len(stocks_list)>0:
                stocks_list["action"] = stocks_list.exchange + "_" + stocks_list.stock
                stocks_list["fecha"]=pd.to_datetime(stocks_list.fecha)
                stocks_list["date"]=pd.to_datetime( stocks_list.date)
                stocks_list = stocks_list.loc[(stocks_list["action"].isin(stock_list_short.action.values))]
                for i in  stocks_list.index:
                    try:
                                fila= stocks_list.loc[i]
                                stock=fila["stock"]
                                fecha=fila["fecha"]
                                date=fila["date"]



                               
                                data = get_fundamentals(

                                stock+"."+exchange,from_date=fecha+timedelta(days=1))
                                if   not data is None and len(data)>=1:
                                    logger.info("main_fundamentals: New data  downloaded to stock {} (from date {})".format(exchange+"."+stock,fecha.date()))
                                    if "currency_symbol" in data.columns:
                                        data.drop(labels="currency_symbol", inplace=True, axis=1)
                                    data = data.dropna(axis=0,
                                                       how='all')
                                    data.index=pd.to_datetime(data.index)
                                    if not data.empty:
                                            stocks.update_bd(data, exchange, stock,"fundamental",bd)

                    except HTTPError as e:
                        logger.error("main_fundamentals: {}, ".format(exchange+"."+stock)+str(e))
            logger.info("main_fundamentals: Data downloaded to table {}".format(tabla))

                 
