import os

os.chdir("../../")
from utils.database import bd_handler
import market_data
from investpy.news import economic_calendar
from investpy.crypto  import get_crypto_historical_data
if __name__ == '__main__':



    bd = bd_handler.bd_handler("market_data")
    market_data.calendar_data(bd)
    market_data.index_data(bd)
    market_data.calendar_next_data(bd)

    market_data.forex_data(bd)
    market_data.commodities_data(bd)
    market_data.bonds_data(bd)
    market_data.crypto_data(bd)




