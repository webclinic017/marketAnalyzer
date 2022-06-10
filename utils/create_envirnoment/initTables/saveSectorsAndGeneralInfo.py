#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 16 12:59:14 2022

@author: manuel
"""
import time
import os
os.chdir("../../../")
import sqlalchemy
import pandas as pd
import mysql.connector
import os
from eod import  EodHistoricalData
import configparser
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config/config_key.properties')
    data_dir = config.get('DIR_STRUCTURE', 'guia_dir')
    directorioALMACENAMIENTO= config.get('DIR_STRUCTURE', 'storage_dir')
    api_key=config.get('DIR_STRUCTURE', 'api_key')
    client = EodHistoricalData(api_key)
    database_username =  config.get('DIR_STRUCTURE', 'database_user')
    database_password =  config.get('DIR_STRUCTURE', 'database_password')
    database_ip       = config.get('DIR_STRUCTURE', 'database_host')+":"+ config.get('DIR_STRUCTURE', 'database_port')
    database_name     =  config.get('DIR_STRUCTURE', 'database_name')
    engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(database_username, database_password,database_ip, database_name),pool_recycle=3600, pool_size=5).connect()
    mydb = mysql.connector.connect(
      host=config.get('DIR_STRUCTURE', 'database_host'),
      user=database_username,
      password=database_password,
      port=config.get('DIR_STRUCTURE', 'database_port'),
      database=database_name
    )
    exchanges=pd.read_csv(data_dir+"exchanges.csv", dtype=str,index_col=1)

    mycursorStocks = mydb.cursor()
    for exchange in exchanges.index:
        print(exchange)
        dataframe=pd.DataFrame(columns=["stock","exchange","sector"])
        k=0
        if os.path.isfile(data_dir+"exchanges/"+exchange+"/stocks.csv"):
            stocks_del_exchange=pd.read_csv(data_dir+"exchanges/"+exchange+"/stocks.csv")

            cols = ",".join([str(i) for i in stocks_del_exchange.columns.tolist()])

            stocks_del_exchange=stocks_del_exchange.loc[stocks_del_exchange["Exchange"].notna()]
            stocks_del_exchange=stocks_del_exchange.fillna(0)
            for i,row in  stocks_del_exchange.iterrows():
                code=row["Code"]
                tipo=row["Type"]
                sql="select * from sectors where stock=%s and exchange=%s"
                mycursorStocks.execute(sql,(code,exchange))
                u= mycursorStocks.fetchall()

                mydb.commit()
                if len(u)==0 and tipo=="Common Stock":

                    try:


                     sql = "INSERT INTO  sectors VALUES (%s,%s,%s)"
                     sector = client.get_fundamental_equity(code+"."+exchange, filter_='General::Sector')
                     print("Code %s,exchange %s,sector %s"%(code,exchange,sector))
                     print(sector)
                     mydb.cursor().execute(sql,(code,exchange,sector))
                     mydb.commit()
                    except Exception as e:
                        pass
                else:
                    print(u)
