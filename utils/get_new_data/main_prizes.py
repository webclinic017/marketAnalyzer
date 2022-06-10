import os
os.chdir("../../")
from utils.get_new_data.stocks_data_EOD_implementation.get_fundamentals import get_fundamentals
from utils.get_new_data.stocks_data_EOD_implementation.get_prizes import  get_eod_prizes
import stocks
from utils.database import bd_handler
from mysql.connector.errors import ProgrammingError
import logging, logging.config
from logging import getLogger
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
if __name__ == "__main__":
        bd=bd_handler.bd_handler("stocks")
        query=      "   select stocks.exchange as exchange, stocks.code  as stock from stocks " \
                    "       inner join degiro on stocks.code=degiro.code and stocks.exchange=degiro.exchange "\
                    "union " \
                    "   select stocks.exchange as exchange, stocks.code   as stock from stocks " \
                    "       inner join admiralmarkets " \
                    "       on stocks.code=admiralmarkets.code and stocks.exchange=admiralmarkets.exchange "\
                    "union " \
                    "   select code as exchange,company as stock  from exchanges " \
                    "       inner join market_data.indice_country on indice_country.country=exchanges.country " \
                    "       inner join indices on indices.indice=indice_country.indice;"
        stock_list=bd.execute_query_dataframe(query)
        exchanges=stock_list.exchange.unique()
        for exchange in exchanges:
            if len(exchange.split(" "))==1:
                stocks.check_exchange(exchange,"precios",get_eod_prizes,bd)
                stocks.check_exchange(exchange, "fundamental", get_fundamentals,bd)
        for i,stock in enumerate(stock_list.values):
            if(i%1000==0):
                logger.info("main_prizes: {} symbols downloaded".format(i))
            try:
                stocks.save_stocks_data(stock[1], stock[0], "precios",bd)
            except ProgrammingError:
                logger.error("main_prizes: ProgrammingError: exchange {}, stock {}".format(stock[0],stock[1]))
            except ConnectionError as e:
                logger.error("main_prizes: ConnectionError: exchange {}, stock {}".format(stock[0], stock[1]))
                raise ConnectionError(e)
            except TypeError as e:
                logger.error("main_prizes: TypeError: exchange {}, stock {}".format(stock[0], stock[1]))

        logger.info("main_fundamental: Data downloaded for exchange {}".format(exchange))


