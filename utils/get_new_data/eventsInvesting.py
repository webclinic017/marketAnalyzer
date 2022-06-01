import configparser
import datetime as dt
from datetime import timedelta
import mysql.connector
import pandas as pd
import sqlalchemy
from investpy import news
import os
os.chdir("../../")
from utils.get_new_data import db_functions
config = configparser.ConfigParser()
config.read('config/config.properties')
def transformarValor(valor=str):
    try:
        if issubclass(type(float(valor)), float):
            return float(valor)
    except Exception as e:
        try:
            if valor[len(valor) - 1] == "%":

                return float(valor[:-1])
            elif valor[len(valor) - 1] == "K":

                return float(valor[:-1]) * 1000
            elif valor[len(valor) - 1] == "M":

                return float(valor[:-1]) * 1000000
        except:


            return None

if __name__=="__main__":
    section='DatabaseInvestingSection'
    mycursor, mydb, _, engine, _= db_functions.init(section)
    TABLE_NAME = config.get('DatabaseInvestingSection', 'database_table_name')
    NUM_DATOS_ACTUALIZAR = int(config.get('DatabaseInvestingSection', 'num_datos_antiguos_actualizar'))
    mycursor.execute("delete  from " + TABLE_NAME + " where  date>=%s;",
                        (dt.date.today() - timedelta(days=NUM_DATOS_ACTUALIZAR),))
    mydb.commit()
    consulta = "select max(date) from " + TABLE_NAME + " ;"
    mycursor.execute(consulta)
    fecha = mycursor.fetchone()[0]
    mydb.commit()
    fecha = dt.datetime.strftime(dt.date.today() - timedelta(days=NUM_DATOS_ACTUALIZAR), "%d/%m/%Y")
    fecha2 = dt.datetime.strftime(dt.datetime.today() + timedelta(days=NUM_DATOS_ACTUALIZAR), "%d/%m/%Y")
    print("Fecha de inicio datos Investing %s" % fecha)
    print("Fecha de fin datos Investing %s" % fecha2)
    calendario = news.economic_calendar(from_date=fecha, to_date=fecha2)
    calendario = calendario.drop(["previous", "time"], axis=1)
    calendario["date"] = pd.to_datetime(calendario["date"], format="%d/%m/%Y")
    calendario.set_index(["id"], drop=True, inplace=True)
    calendario = calendario[~calendario.index.duplicated(keep='first')]
    calendario["actual"] = calendario["actual"].transform(lambda x: transformarValor(x))
    calendario["forecast"] = calendario["forecast"].transform(lambda x: transformarValor(x))
    calendario.to_sql(TABLE_NAME, engine, if_exists="append", chunksize=1000)
