import os

from utils.database import bd_handler
import logging.config
from _mysql_connector import MySQLInterfaceError
from config import load_config

config = load_config.config()
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('errores')
import utils.superset_api.authenticate_superset
import json
import numpy as np
from mysql.connector.errors import ProgrammingError
from config import load_config

config = load_config.config()


def get_charts(session,headers):
    finished=False
    charts=[]
    i=0
    while not finished:
        section = session.get("http://localhost:8088/api/v1/chart/", params={"q": json.dumps({"page_size": 100,"page":i})},
                              headers=headers)

        aux=json.loads(section.text)
        if len(aux["ids"])>0:

            if section.status_code == 200:
                charts = charts+aux["result"]
        else:
            finished=True

        i+=1

    return  charts


def get_dashboards(session,headers):
    finished=False
    dashboards=[]
    i=0
    while not finished:
        section = session.get("http://localhost:8088/api/v1/dashboard/", params={"q": json.dumps({"page_size": 100,"page":i})},
                              headers=headers)

        aux=json.loads(section.text)
        if len(aux["ids"])>0:

            if section.status_code == 200:
                dashboards = dashboards+aux["result"]
        else:
            finished=True

        i+=1

    return  dashboards
