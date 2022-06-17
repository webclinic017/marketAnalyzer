
import requests
import requests_cache
import pandas as pd
from io import StringIO
import datetime
import configparser
import datetime as dt
import logging, logging.config
from logging import getLogger
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('getting_data')
from datetime import timedelta
config = configparser.ConfigParser()
config.read('config/config_key.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD', 'api_key')
expire_after = datetime.timedelta(days=1)
session = requests_cache.CachedSession(
    cache_name="cache", backend="sqlite", expire_after=expire_after)
def get_eod_prizes(ticker="AAPL.US", from_date:dt.date=None, to_date:dt.date=None):
    """

    :param ticker:
    :param from_date: (included)
    :param to_date: (included)
    :return: pd.DataFrame
    """
    url = "https://eodhistoricaldata.com/api/eod/%s" % ticker
    params={}
   
    if not from_date is None:
        params["from"]=from_date
    if not to_date is None:
        params["to"]=to_date
        
    params["api_token"]= api_token
    

    r = session.get(url, params=params)

    if r.status_code == requests.codes.ok:

        df = pd.read_csv(StringIO(r.text), skipfooter=1, parse_dates=[
                         0], index_col=0, engine="python")

        return df

    else:
        logger.error("get_prizes: Failed downloading data for symbol {}".format(ticker))
        return None



