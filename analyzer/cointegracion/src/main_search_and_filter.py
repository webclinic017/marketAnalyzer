import os
os.chdir("../")
from database import  bd_handler
from database import  dabase_functions
from utils import work_dataframes
from functions import estacionaridadYCointegracion
if __name__ == "__main__":

    bd=bd_handler.bd_handler()
    data=bd.execute_consult_dataframe("select * from sectors")
    print(data)
    descr=dabase_functions.filter_by_description("%technology%","%technologies%")
    print(len(descr))
    dataframe=None
    array_dataframes=[]
    for e in descr.values:
        try:
            data=dabase_functions.get_prize_or_fundamenal(e[0],e[1],freq="B")
            data=data.rename(columns={"adjusted_close":e[0]+"_"+e[1]})
            array_dataframes.append(data)
            print(data.tail())
            print(e[0]+"_"+e[1])
            resultadosEst = estacionaridadYCointegracion.analisis_estacionaridad(data.loc[:,e[0]+"_"+e[1]].dropna())
            print(resultadosEst)
            #print(data)
            data1 = dabase_functions.get_prize_or_fundamenal(e[0], e[1],type="fundamental",freq="Q",columna="netincome,totalrevenue")


        except Exception as e:
            print(e)

    data=work_dataframes.merge(array_dataframes)
    print(data.tail())




