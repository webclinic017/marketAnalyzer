import configparser
import mysql.connector
import pandas as pd
import sqlalchemy
from configparser import NoOptionError
pd.options.mode.chained_assignment = None
import os
def init(section,db_name=None):
    print(os.getcwd())
    config = configparser.ConfigParser()
    config.read('config/config.properties')
    host = config.get(section, 'database_host')
    user = config.get(section, 'database_user')
    password = config.get(section, 'database_password')
    port = config.get(section, 'database_port')
    if db_name is None:
        database2 = config.get(section, 'database_name')
    else:
        database2 = config.get(section, db_name)

    data_dir = None
    try:
        data_dir=directorio_almacenamiento= config.get(section, 'data_dir')
    except NoOptionError:
        pass
    try:
        directorio_almacenamiento = config.get(
            'DatabaseStocksSection', 'fundamental_storage_dir')
    except NoOptionError:
        pass
    mydbStocks = mysql.connector.connect(
        host=host,
        user=user,
        port=port,
        password=password, database=database2
    )
    mycursorStocks = mydbStocks.cursor()
    engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
        user,  password,  host + ":" + port, database2), pool_recycle=3600, pool_size=5).connect()
    return mycursorStocks, mydbStocks, data_dir,engine,  directorio_almacenamiento

def executeQuery( mycursorStocks,mydbStocks, sql, params=None):
    if params is None:
        mycursorStocks.execute(sql)
    else:
        mycursorStocks.execute(sql, params)

    data = (mycursorStocks.fetchall())
    mydbStocks.commit()
    return data

def executeQueryDataFrame( mycursorStocks,mydbStocks,sql, params=None):
    if params is None:
        mycursorStocks.execute(sql)
    else:
        mycursorStocks.execute(sql, params)

    data = pd.DataFrame(mycursorStocks.fetchall())
    data.columns = mycursorStocks.column_names
    mydbStocks.commit()
    return data
