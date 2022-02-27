from EOD.getFundamentals import getFundamentals
import saveFundamental_functions
import datetime as dt
from datetime import timedelta
import configparser
import numpy as np
import pandas as pd
config = configparser.ConfigParser()
config.read('../config.properties')
fechaComprobacion =dt.datetime.today()-timedelta(int( config.get('DatabaseStocksSection', 'days_check_fundamental')))
directorioALMACENAMIENTO = config.get('DatabaseStocksSection', 'fundamental_storage_dir')
if __name__ == "__main__":
    mycursorStocks, mydbStocks, data_dir = saveFundamental_functions.init()
    sql="show tables"
    mycursorStocks.execute(sql)
    tablas=mycursorStocks.fetchall()
    mydbStocks.commit()
    tablas=[tabla[0] for tabla in tablas]
    
    
    TABLAS=[tabla for tabla in tablas if (len(tabla.split("_"))==2 and tabla.split("_")[1]=="fundamental") ]
    
    
    for tabla in TABLAS:
       
        try:
            sql="select stock,fecha from {} where fecha>%s"
            mycursorStocks.execute(sql.format(tabla),(fechaComprobacion,))
            stocks=mycursorStocks.fetchall()
            stocks=np.unique([stock[0] for stock in stocks])
            mydbStocks.commit()
            lista=list(stocks)
            import time
            for stock in lista:
                
                try:
                    exchange=tabla.split("_")[0]
                   
                    sql="delete from {} where stock=%s" 
                    mycursorStocks.execute(sql.format(str(tabla)),(str(stock),))
                    mydbStocks.commit()
                    precios = getFundamentals(
                    stock+"."+exchange)
                    name_archivo = directorioALMACENAMIENTO+exchange+"/"+stock+".csv"
                    if not precios is None:
                        if "currency_symbol" in precios.columns:
                            precios.drop(labels="currency_symbol",inplace=True,axis=1)
                        precios = precios.dropna( axis=0, 
                                how='all')
                           
                       
                    
                        
                        if precios is not None:
                            precios.index=pd.to_datetime(precios.index)
                            
                            
                            
                            if not precios.empty:
                             
                                    prize_df = precios
                                    prize_df.sort_index(ascending=True, inplace=True)
                               
                            prize_df.to_csv(name_archivo)
                               
                            precios1 = precios
                                 
                                
                            saveFundamental_functions.UpdateDatosEnBD(precios1,exchange,stock)
                           
                            
                except Exception as f:
                    print(f)
        except Exception as e:
            print(e)
            
      
       
        
     
    
