import os
os.chdir("../../")
import configparser
import investpy
import mysql.connector
import sqlalchemy
config = configparser.ConfigParser()
drop = False
from datetime import timedelta
import time
import datetime as dt
hoy = dt.datetime.today()
hoy_string = dt.datetime.strftime(dt.datetime.today(), "%d/%m/%Y")
get_all_data = True
from utils.get_new_data import db_functions

if __name__ == '__main__':
    section='DatabaseForexSection'
    mycursor, mydb, data_dir, engine,_= db_functions.init(section)

    filtro = {"commodities": ["Crude Oil WTI", "Gold", "Cooper", "Silver"],
              "crypto": ["Bitcoin", "Ethereum", 'Binance Coin', 'Cardano', 'Polkadot'],
              "bonds": ['France 30Y', 'France 10Y', 'France 7Y',
                        'France 5Y', 'France 3Y', 'France 2Y', 'France 1Y', 'France 6M',
                        'France 3M', 'France 1M', 'Germany 10Y',
                        'Germany 30Y', 'Germany 7Y', 'Germany 1Y',
                        'Germany 5Y', 'Germany 3Y', 'Germany 2Y',
                        'Germany 6M', 'Germany 3M', 'U.K. 30Y'
                  , 'U.K. 10Y', 'U.K. 5Y',
                        'U.K. 3Y', 'U.K. 2Y', 'U.K. 1Y', 'U.K. 6M', 'U.K. 3M', 'U.K. 1M', 'U.K. 7Y', 'U.S. 30Y',
                        'U.S. 10Y', 'U.S. 7Y', 'U.S. 5Y', 'U.S. 3Y', 'U.S. 3M', 'U.S. 1Y', 'U.S. 6M', 'U.S. 1M', 'U.S. 2Y',
                        'Spain 30Y', 'Spain 10Y', 'Spain 7Y',
                        'Spain 3Y', 'Spain 2Y', 'Spain 1Y', 'Spain 9M', 'Spain 6M', 'Spain 3M',
                        'Spain 1M', 'Spain 5Y']}

    diccionario_modulos_investpy = {"commodities": [investpy.commodities, investpy.commodities.get_commodities_list,
                                                    investpy.commodities.get_commodity_historical_data],
                                    "crypto": [investpy.crypto, investpy.crypto.cryptos_as_list,
                                               investpy.crypto.get_crypto_historical_data],
                                    "bonds": [investpy.bonds, investpy.bonds.get_bonds_list,
                                              investpy.bonds.get_bond_historical_data]}
    diccionario_modulos_investpy

    if get_all_data:
        for nombre_modulo, modulo in diccionario_modulos_investpy.items():
            array = modulo[1]()

            for com in array:

                if com in filtro[nombre_modulo]:
                    print(com)
                    if drop == True:
                        try:
                            print(com)
                            consulta = "drop table {}".format(com)
                            print(consulta)
                            mycursor.execute(consulta)
                            mydb.commit()
                        except Exception as e:
                            pass

                    consulta = "create table if not exists  {} (fecha date, Open double, High double, Low double, Close double, Volume double,Name varchar(100))".format(
                        nombre_modulo)
                    mycursor.execute(consulta)
                    mydb.commit()

                    consulta_fecha = "select max(fecha) from {} where name=%s".format(nombre_modulo)
                    mycursor.execute(consulta_fecha, (com,))
                    fecha = mycursor.fetchone()[0]
                    mydb.commit()

                    if fecha is None:

                        data = modulo[2](com, from_date="20/01/1998",
                                         to_date=dt.datetime.strftime(hoy - timedelta(days=1), "%d/%m/%Y"))
                        data = data.rename_axis(index={"Date": "fecha"})
                        if "Currency" in data.columns:
                            data = data.drop("Currency", axis=1)
                        data["name"] = com
                        print(data.tail())
                        data.to_sql(nombre_modulo, engine, if_exists="append", chunksize=1000)

                    else:
                        if fecha < (hoy - timedelta(days=1)).date():

                            try:
                                data = modulo[2](com, from_date=dt.datetime.strftime(fecha + timedelta(days=1), "%d/%m/%Y"),
                                                 to_date=hoy_string)
                                data = data.iloc[0:-1]
                                data = data.rename_axis(index={"Date": "fecha"})
                                if "Currency" in data.columns:
                                    data = data.drop("Currency", axis=1)
                                data["name"] = com
                                if len(data) > 0:
                                    print(data.tail())
                                    data.to_sql(nombre_modulo, engine, if_exists="append", chunksize=1000)
                            except Exception as e:
                                print(e)

                    time.sleep(1)
