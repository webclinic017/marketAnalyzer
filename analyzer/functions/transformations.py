#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 20:51:25 2022

@author: manuel
"""
from math import log
def pct_change(df):

    u = 100 * (1 - df[0] / df)
    
def boxcox(landa,u):
    if landa!=0:
        u=(u**landa-1)/landa
    else:
        u=log(u)
    return u
    