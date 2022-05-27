import numpy as np
def MAPE(real, fitted):
    return np.mean(abs((real-fitted)/real))
def SMAPE(real, fitted):
    return np.mean(abs(real-fitted)*2/(abs(real)+abs(fitted)))
