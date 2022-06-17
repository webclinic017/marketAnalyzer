import os
os.chdir("../")
from utils.database import  bd_handler
from utils.database import database_functions
from utils.dataframes import work_dataframes
from functions.estacionaridad import estacionaridadYCointegracion
if __name__ == "__main__":

    bd= bd_handler.bd_handler("stocks")
    data=bd.execute_query_dataframe("select * from sectors")
    print(data)
    descr= database_functions.filter_by_description("%technology%", "%technologies%",bd=bd)
    print(len(descr))
    dataframe=None
    array_dataframes=[]
    for e in descr.values:
        try:
            data= database_functions.get_prize_or_fundamenal(e[0], e[1], freq="B",bd=  bd)
            data=data.rename(columns={"adjusted_close":e[0]+"_"+e[1]})
            array_dataframes.append(data)
            print(data.tail())
            print(e[0]+"_"+e[1])
            resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data.loc[:, e[0] + "_" + e[1]].dropna())
            print(resultadosEst)
            #print(data)
            data1 = database_functions.get_prize_or_fundamenal(e[0], e[1], type="fundamental", freq="Q", columna="netincome,totalrevenue",bd=  bd)


        except Exception as e:
            print(e)

    data= work_dataframes.merge(array_dataframes)
    print(data.tail())




