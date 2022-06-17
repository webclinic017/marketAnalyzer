import numpy as np
import math
def mape(real, fitted):
    return np.mean(abs((real-fitted)/real))
def smape(real, fitted):
    return np.mean(abs(real-fitted)*2/(abs(real)+abs(fitted)))
def rmse(real, fitted):
    return math.sqrt(np.mean((real - fitted)**2))

def all_metricas(real,fitted):
    return {"rmse":rmse(real,fitted),"smape":smape(real,fitted),"mape":mape(real,fitted)}
