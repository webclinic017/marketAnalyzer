import degiroapi
import os
os.chdir("../../../")
import pandas as pd
import configparser
pd.options.mode.chained_assignment = None

if __name__=="__main__":
    config = configparser.ConfigParser()
    config.read('config/config.properties')

    section='DEGIRO_SECTION'
    guia_dir= config.get(section, 'guia_dir')
    guardar_dir=config.get(section, 'guardar_dir')
    cuenta= config.get(section, 'user')
    password=config.get(section, 'password')
    exchanges=pd.read_csv(guia_dir+"exchanges.csv")
    degiro = degiroapi.DeGiro()

    degiro.login( cuenta,password)
    sp5symbols = []
    print(guia_dir,guardar_dir)

    exchangesDict={}
    exchangesDict["Alemania"]=906
    exchangesDict["Austria"]=957
    exchangesDict["Bélgica"]=982
    exchangesDict["Dinamarca"]=975
    exchangesDict["España"]=895
    exchangesDict["Finlandia"]=908
    exchangesDict["Francia"]=886
    exchangesDict["Grecia"]=940
    exchangesDict["Hungria"]=953
    exchangesDict["Irlanda"]=963
    exchangesDict["Italia"]=915
    exchangesDict["Noruega"]=911
    exchangesDict["Paises Bajos"]=978
    exchangesDict["Polonia"]=913
    exchangesDict["Portugal"]=954
    exchangesDict["Reino Unido"]=923
    exchangesDict["República Checa"]=959
    exchangesDict["Suecia"]=899
    exchangesDict["Suiza"]=976
    exchangesDict["Turquia"]=880
    exchangesDict["Australia"]=849
    exchangesDict["Canada"]=845
    exchangesDict["USA"]=846
    exchangesDict["Hong Kong"]=1023
    exchangesDict["Japon"]=905
    exchangesDict["Singapur"]=1030
    productos=None
    codigos=[895,846,906,886,923,908,982,975,940,953,963,915,911]



    for codigo  in exchangesDict.values():

        parada=False
        offf=0
        while parada==False:
         try:
             prods = degiro.get_stock_list(indexId=None, stockCountryId=codigo,offset=offf)
             offf+=1000
             print("Codigo %s, offset %s, numProds %s"%(codigo,offf,len(prods)))


             if len(prods)<1000:
                 parada=True
             if productos is None:
                    productos=pd.DataFrame(prods)
             else:
                    productos2=pd.DataFrame(prods)
                    productos=pd.concat([productos,productos2])

         except  Exception as efff:
            parada=True
            print(efff)



    exchanges=pd.read_csv(guia_dir+"exchanges.csv")
    exchanges.set_index(["Name"],inplace=True,drop=False)

    for e in exchanges.index:


            code=exchanges.loc[e].Code
            stocks_del_exchange=pd.read_csv(guia_dir+"exchanges/"+code+"/stocks.csv",header=0)
            stocks_del_exchange=stocks_del_exchange.loc[ stocks_del_exchange["Isin"].isin(productos["isin"])]
            if not stocks_del_exchange.empty:
                   print(code)
                   try:
                           os.mkdir(guardar_dir+"exchanges/"+code)
                   except Exception as ef:
                         ef
                   #stocks_del_exchange.to_csv(guardar_dir+"exchanges/"+code+"/exchanges.csv")

        
        
            
