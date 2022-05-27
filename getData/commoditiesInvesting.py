#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 19 17:26:19 2022

@author: manuel
"""
import investpy
import configparser
import mysql.connector
import numpy as np
import sqlalchemy
config = configparser.ConfigParser()
drop=False
from datetime import timedelta
import time
config.read('../config.properties')
HOST=config.get('DatabaseForexSection', 'database_host')
USER=config.get('DatabaseForexSection', 'database_user')
PASSWORD=config.get('DatabaseForexSection', 'database_password')
PORT= config.get('DatabaseForexSection', 'database_port')
TABLE = config.get('DatabaseForexSection', 'database_table_name')
DATABASE1= config.get('DatabaseForexSection', 'database_name')
database_username = USER
database_ip = HOST+":"+PORT
commodities=(investpy.commodities.get_commodities_list())
mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        port=PORT,
        password=PASSWORD, database="forex"
    )
import datetime as dt
hoy=dt.datetime.today()
hoy_string=dt.datetime.strftime(dt.datetime.today(),"%d/%m/%Y")
engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(
    USER, PASSWORD, database_ip , DATABASE1), pool_recycle=3600, pool_size=5).connect()

mycursor = mydb.cursor()
#consulta="create table if not exists  commodities (fecha date, Open double, High double, Low double, Close double, Volume double,Name varchar(100))"
#mycursor.execute(consulta)
#mydb.commit()
for com in commodities:
    
    if com in ["Crude Oil WTI","Gold","Cooper","Silver"]:
        print(com)
        if drop==True:
                try:
                    print(com)
                    consulta="drop table {}".format(com)
                    print(consulta)
                    mycursor.execute(consulta)
                    mydb.commit()
                except Exception as e:
                    pass
            
        
        consulta_fecha="select max(fecha) from commodities where name=%s"
        mycursor.execute(consulta_fecha,(com,))
        fecha=   mycursor.fetchone()[0]
        mydb.commit()
      
        if fecha is None:
              
            data=investpy.commodities.get_commodity_historical_data(com,from_date="20/01/1998",to_date=dt.datetime.strftime(hoy-timedelta(days=1),"%d/%m/%Y"))
            data=data.rename_axis(index={"Date":"fecha"})
            data=data.drop("Currency",axis=1)
            data["name"]=com
            print(data.tail())
            data.to_sql("commodities",engine,if_exists="append", chunksize=1000)
           
        else:
            if fecha<(hoy-timedelta(days=1)).date():
           
                try:
                    data=investpy.commodities.get_commodity_historical_data(com,from_date=dt.datetime.strftime(fecha+timedelta(days=1),"%d/%m/%Y"),to_date=hoy_string)
                    data=data.iloc[0:-1]
                    data=data.rename_axis(index={"Date":"fecha"})
                    data=data.drop("Currency",axis=1)
                    data["name"]=com
                    if len(data)>0:
                        print(data.tail())
                        data.to_sql("commodities",engine,if_exists="append", chunksize=1000)
                except Exception as e:
                    print(e)
          
                
        time.sleep(1) 