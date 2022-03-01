#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 14:08:42 2022

@author: manuel
"""
import datetime as dt
def write(message,archivo):
    file=open(archivo,"a")
    file.write("Time: %s Message: %s\n"%(dt.datetime.now().strftime("%m/%d/%Y, %H:%M"),message))
    print("Time: %s Message: %s\n"%(dt.datetime.now().strftime("%m/%d/%Y, %H:%M"),message))
    file.close()