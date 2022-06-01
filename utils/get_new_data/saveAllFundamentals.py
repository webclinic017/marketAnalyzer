import os
os.chdir("../../")
from EOD.getFundamentals import getFundamentals
import saveFundamental_functions
import datetime as dt
import configparser
import pandas as pd
import savePrizes_functions
from utils.get_new_data import db_functions
config = configparser.ConfigParser()
config.read('config/config.properties')
section='DatabaseStocksSection'
fechaLim=dt.date(2021,12,1)

if __name__ == "__main__":
    mycursorStocks, mydbStocks, data_dir,_,directorio_almacenamiento = db_functions.init(section)
    sql="show tables"
    mycursorStocks.execute(sql)
    tablas=mycursorStocks.fetchall()
    mydbStocks.commit()
    tablas=[tabla[0] for tabla in tablas]
    
    #creamos las tablas a partir de las de precios
    TABLAS=[tabla for tabla in tablas if (len(tabla.split("_"))==2 and tabla.split("_")[1]=="precios") ]
    
    print(TABLAS)
    kk=0
    ex_loss=0
    for tabla in TABLAS:

            print(tabla)
            if kk==1:
                print(ex_loss)
            exchange=tabla.split("_")[0]
            savePrizes_functions.comprobarSiExisteExchange(exchange)
            sql = " select code from stocks where exchange=%s;"
            mycursorStocks.execute(sql,(exchange,))
            stocks=pd.DataFrame(mycursorStocks.fetchall())
            mydbStocks.commit()
            for stock in stocks.values:
                        stock=stock[0]
                        consulta="select max(fecha) from {}_fundamental where stock=%s ".format(exchange)
                        mycursorStocks.execute(consulta,(stock,))
                        fecha= mycursorStocks.fetchone()[0]
                        
                        mydbStocks.commit()
                        if fecha is None or fecha<fechaLim:
                            precios = getFundamentals(stock+"."+exchange)
                            name_archivo = directorio_almacenamiento + exchange + "/" + stock + ".csv"
                            if not precios is None:
                                    if "currency_symbol" in precios.columns:
                                        precios.drop(labels="currency_symbol",inplace=True,axis=1)
                                    precios = precios.dropna( axis=0, 
                                            how='all')
                                    precios.index=pd.to_datetime(precios.index)
                                    if not precios.empty:
                                            prize_df = precios
                                            prize_df.sort_index(ascending=True, inplace=True)
                                            prize_df.to_csv(name_archivo)  
                                            precios1=precios
                                            if fecha is not None:
                                                lim=dt.datetime(fecha.year, fecha.month, fecha.day)
                                                precios1 = precios.loc[precios.index>lim]
                                            if len(precios1)>0:
                                                saveFundamental_functions.UpdateDatosEnBD(precios1, exchange, stock)
                                                print(exchange,stock,fecha)
                                                print(precios1)
                                               
                                            else:
                                                print("Empty")
                                            savePrizes_functions.cargarPrecios(stock, exchange)
                        
                           
