import mysql.connector
import math
from  investpy import news
import datetime as dt
import pytz as tz
from datetime import timedelta
import pandas as pd
import operator
import json
import sys
import datetime as dt
import numpy as np
import sqlalchemy
import configparser
config = configparser.ConfigParser()
config.read('../config.properties')
def transformarValor(valor=str):
   try:
    if issubclass(type(float(valor)), float):
        return float(valor)
   except Exception as e:
    try: 
     if valor[len(valor)-1]=="%":
        #print ("Porcentaje")
        return float(valor[:-1])
     elif  valor[len(valor)-1]=="K":
        #print ("K")
        return  float(valor[:-1])*1000
     elif  valor[len(valor)-1]=="M":
        #print ("M")
        return float(valor[:-1])*1000000
    except:
    
         #print (valor)
        return None
class  Bd():
    
    
    def __init__(self):
        self.mydb = mysql.connector.connect(
            host=HOST,
            user=USER,
            port=PORT,
            password=PASSWORD,database=DATABASE
            )
        self.mycursor = self.mydb.cursor()
HOST=config.get('DatabaseInvestingSection', 'database_host')
USER=config.get('DatabaseInvestingSection', 'database_user')
PASSWORD=config.get('DatabaseInvestingSection', 'database_password')
PORT= config.get('DatabaseInvestingSection', 'database_port')
DATABASE= config.get('DatabaseInvestingSection', 'database_name')
TABLE_NAME= config.get('DatabaseInvestingSection', 'database_table_name')
NUM_DATOS_ACTUALIZAR= int(config.get('DatabaseInvestingSection', 'num_datos_antiguos_actualizar'))
database_username = USER
database_password = PASSWORD
database_ip       = HOST+":"+PORT
database_name     = DATABASE
engine = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?charset=utf8'.format(database_username, database_password,database_ip, database_name),pool_recycle=3600, pool_size=5).connect()
bd=Bd()
bd.mycursor.execute("delete  from "+TABLE_NAME+" where  fecha>=%s;",(dt.date.today()-timedelta(days=NUM_DATOS_ACTUALIZAR),))
bd.mydb.commit()
consulta="select max(fecha) from "+TABLE_NAME+" ;"
bd.mycursor.execute(consulta)
fecha=bd.mycursor.fetchone()[0]
bd.mydb.commit()
#fecha=dt.datetime(1998,1,1)
#fecha2=dt.datetime(1999,12,31)
#fecha=dt.datetime.strftime(fecha, "%d/%m/%Y")
#fecha2=dt.datetime.strftime(fecha2, "%d/%m/%Y")
fecha=dt.datetime.strftime(dt.date.today()-timedelta(days=NUM_DATOS_ACTUALIZAR), "%d/%m/%Y")
fecha2=dt.datetime.strftime(dt.datetime.today()+timedelta(days=NUM_DATOS_ACTUALIZAR), "%d/%m/%Y")
print("Fecha de inicio datos Investing %s"%fecha)
print("Fecha de fin datos Investing %s"%fecha2)
calendario=news.economic_calendar(from_date=fecha,to_date=fecha2)
calendario=calendario.drop(["previous","time"],axis=1)
calendario.rename(columns={"date":"fecha","forecast":"esperado"},inplace=True)
calendario["fecha"]=pd.to_datetime(calendario["fecha"],format= "%d/%m/%Y")
#%%Cake2
calendario.set_index(["id"],drop=True,inplace=True)
calendario = calendario[~calendario.index.duplicated(keep='first')]
calendario["actual"]=calendario["actual"].transform(lambda x: transformarValor(x))
calendario["esperado"]=calendario["esperado"].transform(lambda x: transformarValor(x))
calendario.to_sql(TABLE_NAME,engine,if_exists="append", chunksize=1000)
