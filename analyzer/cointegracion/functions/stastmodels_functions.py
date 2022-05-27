

from statsmodels.tsa.stattools import adfuller

def adfuller_(data):
    resultado=adfuller(data,regresults=True)
    lamda = resultado[3].resols.params[0]
    return  resultado,lamda
