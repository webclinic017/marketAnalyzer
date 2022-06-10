import os

os.chdir("../../")
from utils.database import bd_handler
import logging.config
from _mysql_connector import MySQLInterfaceError
from config import load_config
from get_objects import get_charts
config = load_config.config()
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
import json
import utils.superset_api.authenticate_superset as authenticate_superset
import numpy as np
from mysql.connector.errors import ProgrammingError
from config import load_config
from functions_superset import delete_fields_modeL
config = load_config.config()


def create_chart(data):
    if "datasource_name_text" in data.keys():
        data["datasource_name"] = data["datasource_name_text"]
    data=delete_fields_modeL(data)
    data["owners"] = [1]

    respuesta = session.post("http://localhost:8088/api/v1/chart/", headers=headers, json=data)



def comparator_filter(params, nombre_database, section, slices_names):
    """
    Creates a new chart using a schema and changing the name in comparator. The name in the comparator must appear exactly in the chart title (called slice_name) and in the comparator
    to check if the chart is created checking the comparator in the array of titles.

    :param params: chart params from model chart
    :param nombre_database: bd.table to insert data
    :param section: chart schema from model chart
    :param slices_names: charts already created
    """
    try:
        nombre_tipo = params["adhoc_filters"][0]["comparator"]
        if isinstance(params["adhoc_filters"][0]["comparator"], str):
            nombre_tipo = nombre_tipo.replace("%", "")
            query = "select distinct Name from {} where name!=%s".format(nombre_database)
            otros_nombres = bd.execute_query(query, (nombre_tipo,))
            otros_nombres = np.array(otros_nombres).reshape(-1)
            if nombre_tipo not in otros_nombres:
                for n in otros_nombres:
                    i_def = None
                    array = n.split(" ")
                    for i, e in enumerate(array):
                        if e.lower() == nombre_tipo.lower():
                            i_def = i
                            break
                    if i_def is not None:
                        break
                if i_def is not None:
                    otros_nombres = [e.split(" ")[i_def] for e in otros_nombres]

            for nombre in otros_nombres:
                print(nombre)

                data = section.copy()
                if data['slice_name'].replace(nombre_tipo, nombre) not in slices_names:
                    data = json.dumps(data)
                    data = data.replace(nombre_tipo, nombre)
                    data = data.replace("\n", "")
                    data = json.loads(data)
                    create_chart(data)
                    slices_names.append(data['slice_name'])

                else:
                    print("Already saved")
        else:
            pass
    except ProgrammingError as e:
        logger.error(e)
    except MySQLInterfaceError as e:
        logger.error(e)


def time_filter(params, section, slices_names):
    """

    Creates a new chart using a schema and changing the time which uses Last keyword (Last month, Last week etc) . The term "last {period}" must appear exactly in the title in lower case
    to check if the chart is created checking the name in the array of titles.

    :param params: chart params from  modelchart
    :param section:  chart schema from model chart
    :param slices_names: list of char names
    """
    times = ["Last year", "Last month", "Last quarter"]
    filter = params["time_range"]
    if filter.split(" ")[0] == "Last":
        for n in list(set(times) - set((filter,))):
            data = section.copy()
            if data['slice_name'].replace(filter.lower(), n.lower()) not in slices_names:
                data['slice_name'] = data['slice_name'].replace(filter.lower(), n.lower())
                data["params"] = json.loads(data["params"])
                data["params"]["time_range"] = n
                data["params"] = json.dumps(data["params"])
                create_chart(data)
                slices_names.append(data['slice_name'])


def calendar_filter(params, section, slices_names):
    """

       Creates a new chart using a schema and changing the time which uses Last . The event name and the country name must appear all with upper or all with lower

       :param params: chart params from  modelchart
       :param section:  chart schema from model chart
       :param slices_names: list of char names
       """
    name_country = None
    name_event = None
    for filter in params["adhoc_filters"]:
        if filter["subject"] == "zone":
            name_country = filter["comparator"][0]
        if filter["subject"] == "event":
            name_event = filter["comparator"]
            name_event = name_event.replace("%", "")
    query = "select distinct zone from {} where ZONE!=%s".format(nombre_database)
    otros_nombres = bd.execute_query(query, (name_country,))
    otros_nombres = np.array(otros_nombres).reshape(-1)
    events = config["superset"]["list_events"]
    if name_country is not None and name_event is not None:
        for nombre in otros_nombres:



            if    (nombre.split(" ")[0].upper() in config["superset"]["lista_paises"] or nombre.upper()  in config["superset"]["lista_paises"]):

                for event in events:
                    if event=="interest rate decision":
                        a=3
                    data = section.copy()

                    if data['slice_name'].replace(name_country.upper(), nombre.upper()).replace(name_event.upper(),
                                                                                                event.upper()) not in slices_names:
                        data = json.dumps(data)
                        data = data.replace(name_country.upper(), nombre.upper()).replace(name_event.upper(), event.upper())
                        data = data.replace(name_country.lower(), nombre.lower()).replace(name_event.lower(), event.lower())
                        data = data.replace("\n", "")
                        data = json.loads(data)
                        create_chart(data)
                        slices_names.append(data['slice_name'])

                    else:
                        print("Already saved")


if __name__ == "__main__":
    bd = bd_handler.bd_handler("stocks")
    headers, session = authenticate_superset.authenticate()
    get_charts(session,headers)
    slices_names = []

    charts =     get_charts(session,headers)
    for section in charts:
            slices_names.append(section["slice_name"])

    for section in charts:

            nombre_database = section["datasource_name_text"]

            params = json.loads(section["params"])

            if section["datasource_name_text"] == "market_data.indices":

                if section["datasource_name_text"] == "market_data.calendar":
                    calendar_filter(params, section, slices_names)
                    continue
                if 'time_range' in params.keys():
                    time_filter(params, section, slices_names)
                if "adhoc_filters" in params.keys() and len(params["adhoc_filters"]) > 0 and "comparator" in \
                        params["adhoc_filters"][0]:

                    comparator_filter(params, nombre_database, section, slices_names)

                elif params['viz_type'] == "echarts_timeseries":
                    pass
                else:
                    pass
