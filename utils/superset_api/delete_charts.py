import os
os.chdir("../../")
from utils.database import bd_handler
from get_objects import get_charts
import logging, logging.config
from config import load_config

config = load_config.config()
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
import utils.superset_api.authenticate_superset as authenticate_superset
import json

def delete_other_countries(nombre_database,section,session,bd):
    if nombre_database == "market_data.calendar":
        name = section["slice_name"].split(" ")[0]
        nombre_country=name
        query="select distinct zone from market_data.calendar where zone=%s"
        countries= bd.execute_query(query,(name,))
        if name.upper() not in config["superset"]["lista_paises"] and  section["slice_name"] not in config["superset"]["lista_paises"] :
            if countries is not None:
                id = section["id"]
                code = session.delete("http://localhost:8088/api/v1/chart/{}".format(str(id)), headers=headers)


if __name__ == "__main__":
    bd=bd_handler.bd_handler("stocks")
    headers,session=authenticate_superset.authenticate()
    slices_names=[]

    charts=get_charts(session,headers)
    for section in charts:
            slices_names.append(section["slice_name"])

    for section in charts:

          nombre_database=section["datasource_name_text"]
          params=json.loads(section["params"])
          delete_other_countries(nombre_database, section, session,bd)



