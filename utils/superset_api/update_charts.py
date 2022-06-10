import os

from utils.superset_api import authenticate_superset

os.chdir("../../")
from utils.database import bd_handler

import logging, logging.config
from config import load_config
from get_objects import get_charts, get_dashboards
config = load_config.config()
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
from utils.superset_api.functions_superset import delete_fields_modeL
import utils.superset_api.authenticate_superset as authenticate_superset
import json

def update_chart(data):
    id = data["id"]
    data = delete_fields_modeL(data)
    respuesta = session.put("http://localhost:8088/api/v1/chart/{}".format(str(id)), headers=headers,json=data)
    return respuesta

def add_charts_to_macro_dashboards(chart, dashboards, nombre):
    dashboards_aux=[]

    for dashboard in dashboards:

            id=dashboard["id"]
            if dashboard['dashboard_title']==nombre+ " MACRO":
                dashboards_aux.append(id)

    if "dashboards" in chart.keys():
        chart["dashboards"]=chart["dashboards"] + dashboards_aux
    else:
        chart["dashboards"] =  dashboards_aux

    if "id" in chart.keys():
        respuesta = update_chart(chart)




def modify_order(chart):
    params=chart["params"]
    params=json.loads(params)
    params["order_by_cols"]=["[\"Date\", false]"]
    chart["params"]=json.dumps(params)

    if "id" in chart.keys():
        respuesta=update_chart(chart)
        respuesta




def set_zoom_other_equities(section,session):

        name = section["slice_name"]

        id = section["id"]
        params=json.loads(section["params"])
        params["zoomable"]=True
        section["params"]=json.dumps(params)
        update_chart(section)

def modify_like_clause(section,session):

        name = section["slice_name"]

        id = section["id"]
        params=json.loads(section["params"])
        for filter in params["adhoc_filters"]:
            if filter["subject"]=="event":
                if filter["comparator"].split(" ")[0]=="%cpi":
                    filter["comparator"]=filter["comparator"].replace("%cpi","cpi")

                elif filter["comparator"].split(" ")[0]+" "+filter["comparator"].split(" ")[1] == "%retail sales":
                        filter["comparator"] = filter["comparator"].replace("%retail sales", "retail sales")
                elif filter["comparator"].split(" ")[0] + " " + filter["comparator"].split(" ")[1] == "%manufacturing pmi%":
                    filter["comparator"] = filter["comparator"].replace("%manufacturing", "ism manufacturing")


                section["params"]=json.dumps(params)
                update_chart(section)


if __name__ == "__main__":
    bd=bd_handler.bd_handler("stocks")
    headers,session=authenticate_superset.authenticate()
    slices_names=[]
    charts = get_charts(session, headers)
    dashboards= get_dashboards(session, headers)
    add_charts=False
    add_zoom=False
    modify_like_clause=False
    for section in charts:
        slices_names.append(section["slice_name"])

    for section in charts:

          nombre_database=section["datasource_name_text"]
          params=json.loads(section["params"])
          if add_zoom and nombre_database in config["superset"]["list_other_equities"]:
              set_zoom_other_equities( section, session)
          if modify_like_clause and nombre_database == "market_data.calendar" and section["slice_name"].split(" ")[0] == "UNITED" and \
                  section["slice_name"].split(" ")[1] == "STATES":
              modify_like_clause(section.copy(), session)
          if add_charts:
              nombre=        section["slice_name"]
              esta=False
              aux=nombre.split(" ")[0].upper()
              if  aux in config["superset"]["lista_paises"]:
                 esta=True
                 nombre=aux
              if len(nombre.split(" "))>1:
                  aux = nombre.split(" ")[0].upper()+" "+nombre.split(" ")[1].upper()
                  if  aux in config["superset"]["lista_paises"]:
                    esta = True
                    nombre = aux
              if nombre_database == "market_data.calendar" and esta:
                add_charts_to_macro_dashboards(section.copy(),dashboards,nombre)

          if nombre_database == "market_data.calendar" and  section["slice_name"].split(" ")[-1]=="TABLE":


             modify_order(section)





