

from market_data_implementations import  investing_market_data
import datetime as dt
from datetime import timedelta
import time
import logging,logging.config
from logging import getLogger

logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
#columns to return in the dataframe
columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Name"]



# date to start getting data
initial_date="20/01/1990"


# filter to choose the equities for each endpoint
filter = {"commodities": ["Crude Oil WTI", "Gold", "Cooper", "Silver"],
          "crypto": ["Bitcoin", "Ethereum", 'Binance Coin', 'Cardano', 'Polkadot'],
          "bonds": ['France 30Y', 'France 10Y', 'France 7Y',
                    'France 5Y', 'France 3Y', 'France 2Y', 'France 1Y', 'France 6M',
                    'France 3M', 'France 1M', 'Germany 10Y',
                    'Germany 30Y', 'Germany 7Y', 'Germany 1Y',
                    'Germany 5Y', 'Germany 3Y', 'Germany 2Y',
                    'Germany 6M', 'Germany 3M', 'U.K. 30Y'
              , 'U.K. 10Y', 'U.K. 5Y',
                    'U.K. 3Y', 'U.K. 2Y', 'U.K. 1Y', 'U.K. 6M', 'U.K. 3M', 'U.K. 1M', 'U.K. 7Y', 'U.S. 30Y',
                    'U.S. 10Y', 'U.S. 7Y', 'U.S. 5Y', 'U.S. 3Y', 'U.S. 3M', 'U.S. 1Y', 'U.S. 6M', 'U.S. 1M',
                    'U.S. 2Y',
                    'Spain 30Y', 'Spain 10Y', 'Spain 7Y',
                    'Spain 3Y', 'Spain 2Y', 'Spain 1Y', 'Spain 9M', 'Spain 6M', 'Spain 3M',
                    'Spain 1M', 'Spain 5Y'],
          "forex": ["EUR/USD", "EUR/GBP", "USD/GBP", "USD/JPY", "EUR/JPY", "AUD/CAD", "EUR/AUD", "EUR/CAD",
                    "AUD/USD", "CAD/USD","GBP/USD"],
          "indices": ["S&P 500", "FTSE 100", "IBEX 35", "Nasdaq", "CAC 40", "DAX", "FTSE MIB", "AEX", "SMI",
                      "Nikkei 225", "CSI 300", "S&P/TSX 60"]
          }

def add_data_to_database(bd, name, function,equity):

    query = "create table if not exists  {} (Date date, Open double, High double, Low double, Close double, Volume double,Name varchar(100))".format(
       name)
    bd.execute(query)

    date_query = "select max(Date) from {} where name=%s".format(name)
    date = bd.execute_query(date_query, (equity,))[0][0]
    yesterday=dt.datetime.today()-timedelta(days=1)
    yesterday_string = dt.datetime.strftime(yesterday, "%d/%m/%Y")

    if date is None:
        data=function(equity,from_date="20/01/1990",
                                 to_date=dt.datetime.strftime(yesterday - timedelta(days=1), "%d/%m/%Y"))


        bd.bulk_insert(data, name)

    else:
        if date < (yesterday).date():

            try:
                #first not included
                #last included
                data = function(equity,
                                         from_date=dt.datetime.strftime(date, "%d/%m/%Y"),
                                         to_date=yesterday_string)
                data = data.iloc[1:, :]

                bd.bulk_insert(data, name)
                logger.info("market_data: Inserted on {}, equity {}, from date {} to date {}".format(name,equity,date, yesterday_string))

            except Exception as e:
                logger.error("market_data: Error al guardar market_data de {}:{}".format(name,equity))
                time.sleep(3)

    time.sleep(1)

def get(bd,external_data,name):
    equities = external_data.get_list()
    for equity in equities:
        add_data_to_database(bd, name, external_data.get_data, equity)

def index_data(bd):
    name="indices"
    external_data=investing_market_data.external_index_data(filter [name],columns)
    get(bd, external_data,name)

def  forex_data(bd):
    name = "forex"
    external_data = investing_market_data.external_forex_data(filter["forex"], columns)
    get(bd, external_data,name)

def commodities_data(bd):
    name="commodities"
    external_data = investing_market_data.external_commodities_data(filter[name], columns)
    get(bd, external_data,name)

def crypto_data(bd):
    name=   "crypto"
    external_data = investing_market_data.external_crypto_data(filter[name], columns)
    get(bd, external_data,name)

def bonds_data(bd):
    name="bonds"
    external_data = investing_market_data.external_bonds_data(filter[name], columns)
    get(bd, external_data,name)

def calendar_data(bd):
    name = "calendar"
    query = "create table if not exists  {} (id int,Date date, zone varchar(100)," \
            "currency varchar(100),importance varchar(100),event varchar(100)," \
            "actual double, forecast double)".format(
        name)
    bd.execute(query)
    date_delete=dt.date.today()-timedelta(days=7)
    bd.execute("delete from {} where date>=%s".format(name),(date_delete,))
    logger.info("market_data: Delete on {}, from date {}".format(name, date_delete))
    date_query = "select max(Date) from {} ".format(name)
    date = bd.execute_query(date_query)[0][0]
    next_day = dt.datetime.today()+timedelta(days=60)
    next_day_string = dt.datetime.strftime(next_day, "%d/%m/%Y")
    external_data = investing_market_data.external_calendar_data(None, None)

    if date is None:

        data=   external_data.get_data  (from_date="20/01/1990",
                        to_date=next_day)


        bd.bulk_insert(data, name)

    else:
        if date < (next_day).date():

            try:
                data = external_data.get_data(
                                from_date=dt.datetime.strftime(date, "%d/%m/%Y"),
                                to_date=next_day_string)
                data = data.iloc[1:, :]



                bd.bulk_insert(data, name)
                logger.info("market_data: Inserted on {}, from date {} to date {}".format(name, date,next_day_string))

            except Exception as e:
                logger.error("market_data:Error al guardar market_data de {}".format(name))
                time.sleep(3)

    time.sleep(1)

    pass
def calendar_next_data(bd):
    try:
        name = "calendar_next_events"
        query = "create table if not exists  {} (id int,Date date, zone varchar(100)," \
                "currency varchar(100),importance varchar(100),event varchar(100)," \
                "actual double, forecast double)".format(
            name)
        bd.execute(query)
        execute="delete  from {} where 1=1".format(name)
        logger.info("market_data: Delete * from {}".format(name))
        bd.execute(execute)
        external_data = investing_market_data.external_calendar_data(None, None)
        data = external_data.get_data(
            from_date=dt.datetime.strftime(dt.date.today(), "%d/%m/%Y"),
            to_date=dt.datetime.strftime(dt.date.today()+timedelta(days=7), "%d/%m/%Y"),)
        logger.info("market_data: Inserted on {}, from date {} to date {}".format(name,dt.date.today(),dt.date.today()+timedelta(days=7)))
        bd.bulk_insert(data, name)

    except Exception as e:
        logger.error("market_data: Error al guardar market_data de {}".format(name))
        time.sleep(3)

    time.sleep(1)

    pass
