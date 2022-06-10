from investpy.currency_crosses import get_currency_crosses_list
from investpy.currency_crosses import get_currency_cross_historical_data
from investpy.news import economic_calendar
from investpy.indices import get_indices_list
from investpy.indices import get_index_historical_data
from investpy.currency_crosses import get_currency_cross_historical_data
from investpy.commodities import get_commodities_list
from investpy.commodities import  get_commodity_historical_data
from investpy.crypto import  cryptos_as_list
from  investpy.crypto import get_crypto_historical_data
from investpy.bonds import get_bonds_list
from investpy.bonds import get_bond_historical_data
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
from utils.database import bd_handler
class external_market_data():

    def __init__(self,filter, columns, func_list,func_historical):
        self.func_list=func_list
        self.func_historical = func_historical
        self.filter=filter
        self.columns=columns



    def transform_data(self,data,equity):

        data.rename_axis(index={data.index.names[0]: "Date"})
        for column in data.columns:
            if column not in self.columns:
                data.drop(column, inplace=True, axis=1)
            else:
                data[column] = data[column].astype(float)

        data["name"] = equity
        return data
    def get_list(self):
        """
        Returns the list of equities aplying a filter

        :param filtro: array of filters
        :return: array with equities
        rtype: np.ndarray
        """
        if self.func_list is not None:
            array = self.func_list()
            if self.filter is not None:
                array = list(set(array).intersection(set(self.filter)))
            return array
        return []
    def get_data(self, equity:np.ndarray,  from_date:str,to_date:str):
        """

        :param equities: list of equities to get
        :param fechaI: first date
        :param fechaF: end date
        :return: datafram with historical data
        rtype: pd.DataFrame
        """

        data = self.func_historical(equity, from_date= from_date,
                                 to_date=to_date)
        data = self.transform_data(data,equity)

        return data




class external_index_data(external_market_data):
    def __init__(self,filter,columns):
        super().__init__(filter, columns, get_indices_list,  get_index_historical_data )

    def get_data(self, equity:np.ndarray,  from_date:str,to_date:str):
        """

        :param equities: list of equities to get
        :param fechaI: first date
        :param fechaF: end date
        :return: datafram with historical data
        rtype: pd.DataFrame
        """

        bd=bd_handler.bd_handler("market_data")
        country=bd.execute_query("select country from indice_country where indice=%s",(equity,))[0][0]
        if country=="USA":
            country="united states"
        if country == "UK":
                country = "united kingdom"
        data = self.func_historical(equity,country=country, from_date= from_date,
                                 to_date=to_date)
        data = self.transform_data(data,equity)


        return data

class  external_forex_data(external_market_data):
    def __init__(self,filter,columns):
        super().__init__(filter, columns, get_currency_crosses_list, get_currency_cross_historical_data)

class  external_commodities_data(external_market_data):
    def __init__(self, filter, columns):
        super().__init__(filter, columns,  get_commodities_list, get_commodity_historical_data)

class external_crypto_data(external_market_data):
    def __init__(self, filter, columns):
        super().__init__(filter, columns, cryptos_as_list, get_crypto_historical_data)

class external_bonds_data(external_market_data):
    def __init__(self, filter, columns):
        super().__init__(filter, columns, get_bonds_list, get_bond_historical_data)

def transformarValor(valor=str):
    try:
        if issubclass(type(float(valor)), float):
            return float(valor)
    except Exception as e:
        try:
            if valor[len(valor) - 1] == "%":

                return float(valor[:-1])
            elif valor[len(valor) - 1] == "K":

                return float(valor[:-1]) * 1000
            elif valor[len(valor) - 1] == "M":

                return float(valor[:-1]) * 1000000
        except:


            return None

class external_calendar_data(external_market_data):

    def __init__(self, filter, columns):
        super().__init__(filter, columns, None, None)

    def get_data(self, from_date: str, to_date: str):
        """

        :param equities: list of equities to get
        :param from_date: first date
        :param to_date: end date
        :return: datafram with historical data
        rtype: pd.DataFrame
        """
        calendario= economic_calendar(from_date=from_date, to_date=to_date)
        calendario = calendario.drop(["previous", "time"], axis=1)
        calendario["date"] = pd.to_datetime(calendario["date"], format="%d/%m/%Y")
        calendario.set_index(["id"], drop=True, inplace=True)
        calendario = calendario[~calendario.index.duplicated(keep='first')]
        calendario["actual"] = calendario["actual"].transform(lambda x: transformarValor(x))
        calendario["forecast"] = calendario["forecast"].transform(lambda x: transformarValor(x))


        return calendario

