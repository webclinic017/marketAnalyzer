import os
os.chdir("../../")
from utils.get_new_data.stocks_data_EOD_implementation.get_fundamentals import get_fundamentals
from utils.database import bd_handler
import stocks
import datetime as dt
from datetime import timedelta
import pandas as pd
from logging import getLogger
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
from requests.exceptions import HTTPError
fecha_update=dt.datetime.today()-timedelta(days=100)
if __name__ == "__main__":
    bd=bd_handler.bd_handler("stocks")


    sql="show tables"
    tablas=bd.execute_query(sql)
    tablas=[tabla[0] for tabla in tablas]
    
    
    tablas=[tabla for tabla in tablas if (len(tabla.split("_")) == 2 and tabla.split("_")[1] == "fundamental")]
    

    for tabla in tablas:
            exchange=tabla.split("_")[0]
            sql="select stock,fecha,netincome,totalrevenue,totalassets,totalliab from {} where fecha>%s and (netIncome is null or totalAssets is null or totalrevenue is null or totalliab is null)"
            stocks_list=bd.execute_query_dataframe(sql.format(tabla), (fecha_update,))
            if stocks_list is not None and len(stocks_list) > 0:
                for i in stocks_list.index:
                    try:

                            fila=stocks_list.loc[i]
                            stock=fila["stock"]
                            fecha=fila["fecha"]
                            sql="select   stock,fecha,netincome,totalrevenue,totalassets,totalliab from {} where fecha=%s and stock=%s"
                            data=bd.execute_query_dataframe(sql.format(tabla),(fecha.replace(year=fecha.year-1),stock))
                            if data is not None and len(data)>0:


                                nas=False
                            if len(data)==0:
                                    nas=True
                            else:
                                    for k in ["netincome","totalrevenue","totalassets","totalliab" ]:
                                        if data.loc[0,k] is  not None and  fila[k] is None:
                                            nas=True
                                            break
                            if nas:




                                    exchange=tabla.split("_")[0]
                                    data = get_fundamentals(
                                    stock+"."+exchange,from_date=fecha_update)


                                    if not data is None and len(data)>0:

                                        if "currency_symbol" in data.columns:
                                            data.drop(labels="currency_symbol", inplace=True, axis=1)
                                        data = data.dropna(axis=0,
                                                           how='all')
                                        data.index=pd.to_datetime(data.index)
                                        if not data.empty:
                                                sql="delete from {} where stock=%s and fecha>%s"
                                                bd.execute(sql.format(str(tabla)), (str(stock), fecha_update))
                                                logger.info(
                                                    "main_results: Data deleted in {} from stock {}, since date {}".format(str(tabla),stock,
                                                        fecha_update.date()))
                                                logger.info(
                                                    "main_fundamentals_update: New data  downloaded to stock {} (from date {})".format(
                                                        exchange + "." + stock, fecha_update.date()))

                                                stocks.update_bd(data, exchange, stock, "fundamental",bd)

                    except HTTPError as e:
                        logger.error("main_fundamentals_update: {}, ".format(exchange + "." + stock) + str(e))
                logger.info("main_fundamentals_update: Data downloaded to table {}".format(tabla))



       
      
       
        
     
    
