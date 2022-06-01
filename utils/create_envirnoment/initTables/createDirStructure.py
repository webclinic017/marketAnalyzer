import os
os.chdir("../../../")
import pandas as pd
import urllib.request as request
import json
import os
import configparser
if __name__=="__main__":
    config = configparser.ConfigParser()
    config.read('config/config.properties')
    data_dir = config.get('DIR_STRUCTURE', 'guia_dir')
    directorioALMACENAMIENTO= config.get('DIR_STRUCTURE', 'storage_dir')
    API_KEY=config.get('DIR_STRUCTURE', 'api_key')
    print(directorioALMACENAMIENTO)

    r = request.Request("https://eodhistoricaldata.com/api/exchanges-list/?api_token="+API_KEY+"&fmt=json")
    response = request.urlopen(r)
    response_dict = json.loads(response.read())
    columns=list(response_dict[0].keys())
    columns
    dataframe=pd.DataFrame(columns=columns)
    for u in response_dict:

        columnas=[u[i] for i in columns]

        dataframe.loc[len(dataframe)]=columnas
    dataframe.set_index(["Name"],inplace=True,drop=True)
    #dataframe.to_csv(data_dir+"exchanges.csv")

    try:
        os.mkdir(directorioALMACENAMIENTO+"/precios/")
    except Exception as e:
        pass
    try:
        os.mkdir(directorioALMACENAMIENTO+"/fundamental/")
    except Exception as e:
        pass

    exchanges=dataframe

    for exchange in exchanges.index:
        print(exchange)
        code=dataframe.loc[exchange].Code

        r = request.Request("https://eodhistoricaldata.com/api/exchange-symbol-list/"+code+"/?api_token="+API_KEY+"&fmt=json")
        response = request.urlopen(r)
        response_dict = json.loads(response.read())
        columns=list(response_dict[0].keys())
        dataframe1=pd.DataFrame(columns=columns)

        for u in response_dict:

            columnas=[u[i] for i in columns]
            dataframe1.loc[len(dataframe1)]=columnas
        dataframe1.set_index(["Name"],inplace=True,drop=True)





        try:
            os.mkdir(directorioALMACENAMIENTO+"precios/"+code)
            os.mkdir(directorioALMACENAMIENTO+"fundamental/"+code)
        except Exception as e:
            print(e)
            continue
