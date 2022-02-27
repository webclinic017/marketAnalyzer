
import requests
import requests_cache
import pandas as pd
from io import StringIO
import datetime
import configparser
config = configparser.ConfigParser()
config.read('../config.properties')
pd.options.mode.chained_assignment = None
api_token =config.get('EOD_SECTION', 'api_key')
expire_after = datetime.timedelta(days=1)
session = requests_cache.CachedSession(
    cache_name="cache", backend="sqlite", expire_after=expire_after)
def get_eod_data(symbol="AAPL.US",date=None):

    

    url = "https://eodhistoricaldata.com/api/eod/%s" % symbol
    params={}
   
    if not date is  None:
        params["from"]=date
        
    params["api_token"]= api_token
    

    r = session.get(url, params=params)

    if r.status_code == requests.codes.ok:

        df = pd.read_csv(StringIO(r.text), skipfooter=1, parse_dates=[
                         0], index_col=0, engine="python")

        return df

    else:
        print("Fallo al obtener %s"%symbol)
        return None



