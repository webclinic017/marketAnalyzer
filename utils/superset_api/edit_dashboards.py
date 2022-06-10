import os

os.chdir("../../")
from utils.database import bd_handler
import logging.config
from _mysql_connector import MySQLInterfaceError
from config import load_config
from get_objects import get_dashboards, get_charts
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


if __name__ == "__main__":
    bd = bd_handler.bd_handler("stocks")
    headers, session = authenticate_superset.authenticate()

    dashboards =get_dashboards(session,headers)
    charts = get_charts(session, headers)
    for dashboard in dashboards:
        if dashboard["dashboard_title"]==config["superset"]["title_ref_macro"]:
            model=dashboard
    for dashboard in dashboards:
        dashboard
        aux=dashboard["position_json"]






