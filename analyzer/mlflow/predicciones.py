#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 20 18:12:30 2022

@author: manuel
"""
class InfoCaso:
    def __init__(self,exchange,stock,run_id,version):
        self.__exchange=exchange
        self.__stock=stock
        self.__run_id=run_id
        self.__version=version
        
    @property
    def sector(self):
        return self.__sector
    @sector.setter
    def sector(self,sector):
        self.__sector=sector
    @property
    def exchange(self):
        return self.__exchange
    @exchange.setter
    def exchange(self,exchange):
        self.__exchange=exchange
    @property
    def stock(self):
        return self.__stock
    @stock.setter
    def stock(self,stock):
        self.__stock=stock
    @property
    def run_id(self):
        return self.__run_id
    @run_id.setter
    def run_id(self,run_id):
        self.__run_id=run_id
    @property
    def version(self):
        return self.__version
    @version.setter
    def version(self,version):
        self.__version=version
    def __str__(self):
        return self.__exchange+" "+self.stock
        
        