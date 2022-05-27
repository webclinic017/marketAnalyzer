#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 14:10:51 2022

@author: manuel
"""
import numpy as np
def MAPE(real,fitted):
    return np.mean(abs((real-fitted)/real))


def SMAPE(real,fitted):
     return np.mean(abs(real-fitted)*2/(real+fitted))