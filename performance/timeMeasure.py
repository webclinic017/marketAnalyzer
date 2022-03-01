#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 14:57:40 2022

@author: manuel
"""
import time
class TimeMeasure():
    def __init__(self,):
        self.time1=time.time()
        
    def getTime(self):
        return  time.time()-self.time1