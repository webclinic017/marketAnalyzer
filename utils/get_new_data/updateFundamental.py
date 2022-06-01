import os
os.chdir("../../")
from utils.get_new_data.EOD.getFundamentals import getFundamentals
import saveFundamental_functions
import datetime as dt
import configparser
import pandas as pd
from utils.get_new_data import db_functions
fechaUpdate=dt.datetime(2021,12,30)
section='DatabaseStocksSection'
if __name__ == "__main__":
    mycursorStocks, mydbStocks, data_dir,_,directorio_almacenamiento = db_functions.init(section)
    sql="show tables"
    mycursorStocks.execute(sql)
    tablas=mycursorStocks.fetchall()
    mydbStocks.commit()
    tablas=[tabla[0] for tabla in tablas]
    
    
    TABLAS=[tabla for tabla in tablas if (len(tabla.split("_"))==2 and tabla.split("_")[1]=="fundamental") ]
    
    print(TABLAS)
    for tabla in TABLAS:
            exchange=tabla.split("_")[0]
            sql="select stock,fecha,netincome,totalrevenue,totalassets,totalliab from {} where fecha>%s and (netIncome is null or totalAssets is null or totalrevenue is null or totalliab is null)"
            mycursorStocks.execute(sql.format(tabla),(fechaUpdate,))
            stocks=pd.DataFrame(mycursorStocks.fetchall())
            mydbStocks.commit()
            if len(stocks)>0:
                stocks.columns=  mycursorStocks.column_names
                stocks=stocks.groupby("stock",as_index=False).last()
                stocks["fecha"]=pd.to_datetime(stocks.fecha)
                for i in stocks.index:
                   
                        fila=stocks.loc[i]
                        stock=fila["stock"]
                        fecha=fila["fecha"]
                        sql="select   stock,fecha,netincome,totalrevenue,totalassets,totalliab from {} where fecha=%s and stock=%s"
                        mycursorStocks.execute(sql.format(tabla),(fecha.replace(year=fecha.year-1),stock))
                        data=pd.DataFrame(mycursorStocks.fetchall())
                        if len(data)>0:
                            data.columns= mycursorStocks.column_names
                            mydbStocks.commit()
                            nas=False
                            if len(data)==0:
                                nas=True
                            else:
                                for k in ["netincome","totalrevenue","totalassets","totalliab" ]:
                                    if data.loc[0,k] is  not None and  fila[k] is None:
                                        nas=True
                                        break
                            print(nas) 
                            if nas:
                                exchange=tabla.split("_")[0]
                                precios = getFundamentals(
                                stock+"."+exchange)
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
                                            precios1 = precios.loc[precios.index>fechaUpdate]
                                            sql="delete from {} where stock=%s and fecha>%s" 
                                            mycursorStocks.execute(sql.format(str(tabla)),(str(stock),fechaUpdate))
                                            mydbStocks.commit()
                                            saveFundamental_functions.UpdateDatosEnBD(precios1, exchange, stock)
                                            print(exchange,stock,precios1.loc[:,["netIncome","totalAssets","totalLiab","totalRevenue"]])
                         
                
       
      
       
        
     
    
