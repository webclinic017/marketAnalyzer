#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 09:59:16 2022

@author: manuel
"""



import configparser
import warnings
import pandas as pd
import mysql.connector
import time
import sys
import numpy as np
sys.path.append("../../performance")
sys.path.append("../../writer")
import writer
from timeMeasure import TimeMeasure
config = configparser.ConfigParser()
config.read('../../config.properties')
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
HOST = config.get('DatabaseMacroSection', 'database_host')
USER = config.get('DatabaseMacroSection', 'database_user')
PASSWORD = config.get('DatabaseMacroSection', 'database_password')
PORT = config.get('DatabaseMacroSection', 'database_port')
DATABASE = config.get('DatabaseMacroSection', 'database_name')
PERFORMANCE = True if config.get('PERFORMANCE', 'time') == "True" else False
mydbStocks = mysql.connector.connect(
    host=HOST,
    user=USER,
    port=PORT,
    password=PASSWORD, database=DATABASE
)
mycursorCal = mydbStocks.cursor()


def devolverCalendario(exchange):
    
   
      
    sql="select * from calendario where actual is not null order by fecha asc"
    mycursorCal.execute(sql)
    calendario = pd.DataFrame(mycursorCal.fetchall())
    if len(calendario)>0:
        calendario.columns = mycursorCal.column_names
        
        calendario["fecha"]=pd.to_datetime(calendario["fecha"])
        calendario=calendario.dropna()
    
    

    import re
    calendario["event2"]=calendario["event"].transform(lambda x: re.sub("\([^\\(]*\)","",re.sub("\(YoY\)","YoY",re.sub("\(MoM\)","MoM",re.sub("\(QoQ\)","QoQ",x)))).strip())
    
    calendario=calendario.dropna()
   

   
    calendario["fecha"]=calendario["fecha"].transform(lambda x:x.replace(day=1))
    
    
        #preparar calendario
    calendario.set_index(["fecha"],inplace=True)
    if exchange=="US":
        cpimom=calendario.loc[(calendario["event2"]=="CPI MoM")&(calendario["zone"]=="united states"),"actual"]
        joblessclaims=calendario.loc[(calendario["event2"]=="Jobless Claims 4-Week Avg.")&(calendario["zone"]=="united states"),"actual"]
        tenyearauction=calendario.loc[(calendario["event2"]=="10-Year Note Auction")&(calendario["zone"]=="united states"),"actual"]
        threeyearauction=calendario.loc[(calendario["event2"]=="3-Year Note Auction")&(calendario["zone"]=="united states"),"actual"]
        crudeoilinventories=calendario.loc[(calendario["event2"]=='Crude Oil Inventories')&(calendario["zone"]=="united states"),["actual"]].groupby("fecha").mean()
        crudeoilinventories=crudeoilinventories/100000
        cpiyoy=calendario.loc[(calendario["event2"]=="CPI YoY")&(calendario["zone"]=="united states"),"actual"]
        interestrateporyection1=calendario.loc[(calendario["event2"]== 'Interest Rate Projection - 1st Yr')&(calendario["zone"]=="united states"),"actual"]
        interestrateporyection2=calendario.loc[(calendario["event2"]=='Interest Rate Projection - 2nd Yr')&(calendario["zone"]=="united states"),"actual"]
        interestrateporyectioncurrent=calendario.loc[(calendario["event2"]== 'Interest Rate Projection - Current')&(calendario["zone"]=="united states"),"actual"]
        retailsalesyoy=calendario.loc[(calendario["event2"]=="Retail Sales YoY")&(calendario["zone"]=="united states"),"actual"]
        ismmanufacturingPMI=calendario.loc[(calendario["event2"]=="ISM Manufacturing PMI")&(calendario["zone"]=="united states"),"actual"]
        ismnonmanufacturingPMI=calendario.loc[(calendario["event2"]=='ISM Non-Manufacturing PMI')&(calendario["zone"]=="united states"),"actual"]
        pendinghomesalesindex=calendario.loc[(calendario["event2"]=="Pending Home Sales Index")&(calendario["zone"]=="united states"),"actual"]
        pendinghomesalesmom=calendario.loc[(calendario["event2"]=="Pending Home Sales MoM")&(calendario["zone"]=="united states"),"actual"]
        industrialproductionyoy=calendario.loc[(calendario["event2"]=="Industrial Production YoY")&(calendario["zone"]=="united states"),"actual"]
        interestrate=calendario.loc[(calendario["event2"]=="Fed Interest Rate Decision")&(calendario["zone"]=="united states"),"actual"]
        bloombergconsumerconfidence=calendario.loc[(calendario["event2"]=="Bloomberg Consumer Confidence")&(calendario["zone"]=="united states"),"actual"]
        buildingpermitsmom=calendario.loc[(calendario["event2"]=="Building Permits MoM")&(calendario["zone"]=="united states"),"actual"]
        buildingpermits=calendario.loc[(calendario["event2"]=="Building Permits")&(calendario["zone"]=="united states"),"actual"]
        joltjobsopenings=calendario.loc[(calendario["event2"]=="JOLTs Job Openings")&(calendario["zone"]=="united states"),"actual"]
        gdpqoq=calendario.loc[(calendario["event2"]=="GDP QoQ")&(calendario["zone"]=="united states"),"actual"]
        np.unique(calendario.loc[(calendario["zone"]=="united states"),"event2"])
        calendario2=pd.DataFrame(index=np.unique(calendario.index))
        calendario2["cpimom"]=cpimom
        calendario2["cpiyoy"]=cpiyoy
        #calendario2["retailsalesyoy"]=retailsalesyoy[~retailsalesyoy.index.duplicated(keep='first')]
        calendario2["ismmanufacturingPMI"]=ismmanufacturingPMI
        calendario2["ismnonmanufacturingPMI"]=ismnonmanufacturingPMI
        calendario2["pendinghomesalesindex"]=pendinghomesalesindex
        calendario2["pendinghomesalesmom"]=pendinghomesalesmom[~pendinghomesalesmom.index.duplicated(keep='first')]
        calendario2["crudeoilinventories"]=crudeoilinventories[~crudeoilinventories.index.duplicated(keep='first')]
        calendario2["auction"]=tenyearauction-threeyearauction
        calendario2["joblessclaims"]=joblessclaims[~joblessclaims.index.duplicated(keep='first')]
        calendario2["interestrate"]=interestrate[~interestrate.index.duplicated(keep='first')]
        #calendario2["industrialproductionyoy"]=industrialproductionyoy[~industrialproductionyoy.index.duplicated(keep='first')]
        calendario2["bloombergconsumerconfidencey"]=bloombergconsumerconfidence[~bloombergconsumerconfidence.index.duplicated(keep='first')]
        calendario2["buildingpermits"]=buildingpermits[~buildingpermits.index.duplicated(keep='first')]
        calendario2["buildingpermitsmom"]=buildingpermitsmom[~buildingpermitsmom.index.duplicated(keep='first')]
        calendario2["joltjobsopenings"]=joltjobsopenings[~joltjobsopenings.index.duplicated(keep='first')]
        calendario2["gdpqoq"]=gdpqoq[~gdpqoq.index.duplicated(keep='first')]
        #calendario2["interestrateporyection1"]=interestrateporyection1[~interestrateporyection1.index.duplicated(keep='first')]
        #calendario2["interestrateporyection2"]=interestrateporyection2[~interestrateporyection2.index.duplicated(keep='first')]
        #calendario2["interestrateporyectioncurrent"]=interestrateporyectioncurrent[~interestrateporyectioncurrent.index.duplicated(keep='first')]
        


        
    if exchange=="XETRA":
        cpimom=calendario.loc[(calendario["event2"]=="German CPI MoM")&(calendario["zone"]=="germany"),"actual"]
        cpiyoy=calendario.loc[(calendario["event2"]=="German CPI YoY")&(calendario["zone"]=="germany"),"actual"]
        unemploymentchange=calendario.loc[(calendario["event2"]=='German Unemployment Change')&(calendario["zone"]=="germany"),"actual"]
        unemploymentrate=calendario.loc[(calendario["event2"]== 'German Unemployment Rate')&(calendario["zone"]=="germany"),"actual"]
        retailsalesyoy=calendario.loc[(calendario["event2"]=="German Retail Sales YoY")&(calendario["zone"]=="germany"),"actual"]
        manufacturingPMI=calendario.loc[(calendario["event2"]=="German Manufacturing PMI")&(calendario["zone"]=="germany"),"actual"]
        servicesPMI=calendario.loc[(calendario["event2"]=='German Services PMI')&(calendario["zone"]=="germany"),"actual"]
        industrialProductionMoM=calendario.loc[(calendario["event2"]=="German Industrial Production MoM")&(calendario["zone"]=="germany"),"actual"]
        germanzeweconomicsentiment=calendario.loc[(calendario["event2"]=='German ZEW Economic Sentiment')&(calendario["zone"]=="germany"),"actual"]
        germancurrentconditions=calendario.loc[(calendario["event2"]=='German ZEW Current Conditions')&(calendario["zone"]=="germany"),"actual"]
        germanhousepriceindex=calendario.loc[(calendario["event2"]=='German House Price Index MoM')&(calendario["zone"]=="germany"),"actual"]
        calendario2=pd.DataFrame(index=np.unique(calendario.index))
        calendario2["cpimom"]=cpimom[~cpimom.index.duplicated(keep='first')]
        calendario2["cpiyoy"]=cpiyoy[~cpiyoy.index.duplicated(keep='first')]
        calendario2["retailsalesyoy"]=retailsalesyoy[~retailsalesyoy.index.duplicated(keep='first')]
        calendario2["unemploymentchange"]=unemploymentchange[~unemploymentchange.index.duplicated(keep='first')]
        calendario2["unemploymentrate"]=unemploymentrate[~unemploymentrate.index.duplicated(keep='first')]
        calendario2["manufacturingPMI"]=manufacturingPMI[~manufacturingPMI.index.duplicated(keep='first')]
        calendario2["industrialProductionMoM"]=industrialProductionMoM[~industrialProductionMoM.index.duplicated(keep='first')]
        calendario2["servicesPMI"]=servicesPMI[~servicesPMI.index.duplicated(keep='first')]
        calendario2["germanzeweconomicsentiment"]=germanzeweconomicsentiment[~germanzeweconomicsentiment.index.duplicated(keep='first')]
        calendario2["germancurrentconditions"]=germancurrentconditions[~germancurrentconditions.index.duplicated(keep='first')]
        #calendario2["germanhousepriceindex"]=germanhousepriceindex[~germanhousepriceindex.index.duplicated(keep='first')]
    if exchange=="MC":
        cpimom=calendario.loc[(calendario["event2"]=="Spanish CPI MoM")&(calendario["zone"]=="spain"),"actual"]
        cpiyoy=calendario.loc[(calendario["event2"]=="Spanish CPI YoY")&(calendario["zone"]=="spain"),"actual"]
        unemploymentchange=calendario.loc[(calendario["event2"]=='Spanish Unemployment Change')&(calendario["zone"]=="spain"),"actual"]
        unemploymentrate=calendario.loc[(calendario["event2"]== 'Spanish Unemployment Rate')&(calendario["zone"]=="spain"),"actual"]
        retailsalesyoy=calendario.loc[(calendario["event2"]=="Spanish Retail Sales YoY")&(calendario["zone"]=="spain"),"actual"]
        manufacturingPMI=calendario.loc[(calendario["event2"]=="Spanish Manufacturing PMI")&(calendario["zone"]=="spain"),"actual"]
        servicesPMI=calendario.loc[(calendario["event2"]=='Spanish Services PMI')&(calendario["zone"]=="spain"),"actual"]
        industrialProductionYoY=calendario.loc[(calendario["event2"]=="Spanish Industrial Production YoY")&(calendario["zone"]=="spain"),"actual"]
        newordersyoy=calendario.loc[(calendario["event2"]=="Spanish industrial New Orders YoY")&(calendario["zone"]=="spain"),"actual"]
        consumerconfidence=calendario.loc[(calendario["event2"]=="Spanish Consumer Confidence")&(calendario["zone"]=="spain"),"actual"]
        gdpyoy=calendario.loc[(calendario["event2"]=="Spanish GDP YoY")&(calendario["zone"]=="spain"),"actual"]
        gdpqoq=calendario.loc[(calendario["event2"]=="Spanish GDP QoQ")&(calendario["zone"]=="spain"),"actual"]
        auction=calendario.loc[(calendario["event2"]=="Spanish 10-Year Obligacion Auction")&(calendario["zone"]=="spain"),"actual"]-calendario.loc[(calendario["event2"]=="Spanish 2-Year Bonos Auction")&(calendario["zone"]=="spain"),"actual"]
        calendario2=pd.DataFrame(index=np.unique(calendario.index))
        calendario2["cpimom"]=cpimom[~cpimom.index.duplicated(keep='first')]
        calendario2["cpiyoy"]=cpiyoy[~cpiyoy.index.duplicated(keep='first')]
        calendario2["retailsalesyoy"]=retailsalesyoy[~retailsalesyoy.index.duplicated(keep='first')]
        calendario2["unemploymentchange"]=unemploymentchange
        #calendario2["unemploymentrate"]=unemploymentrate
        calendario2["manufacturingPMI"]=manufacturingPMI
        calendario2["industrialProductionYoY"]=industrialProductionYoY[~industrialProductionYoY.index.duplicated(keep='first')]
        calendario2["servicesPMI"]=servicesPMI
        
        #calendario2["newordersyoy"]=newordersyoy[~newordersyoy.index.duplicated(keep='first')]
        #calendario2["consumerconfidence"]=consumerconfidence[~consumerconfidence.index.duplicated(keep='first')]
        calendario2["gdpyoy"]=gdpyoy[~gdpyoy.index.duplicated(keep='first')]
        calendario2["gdpqoq"]=gdpqoq[~gdpqoq.index.duplicated(keep='first')]
        #calendario2["auction"]=auction[~auction.index.duplicated(keep='first')]

    if exchange=="LSE":
        cpimom=calendario.loc[(calendario["event2"]=="CPI MoM")&(calendario["zone"]=="united kingdom"),"actual"]
        cpiyoy=calendario.loc[(calendario["event2"]=="CPI YoY")&(calendario["zone"]=="united kingdom"),"actual"]
        gfkcomsumerconfidence=calendario.loc[(calendario["event2"]=='GfK Consumer Confidence')&(calendario["zone"]=="united kingdom"),"actual"]
        unemploymentrate=calendario.loc[(calendario["event2"]== 'Unemployment Rate')&(calendario["zone"]=="united kingdom"),"actual"]
        retailsalesyoy=calendario.loc[(calendario["event2"]=="Retail Sales YoY")&(calendario["zone"]=="united kingdom"),"actual"]
        retailsalesmom=calendario.loc[(calendario["event2"]=="Retail Sales MoM")&(calendario["zone"]=="united kingdom"),"actual"]
        manufacturingPMI=calendario.loc[(calendario["event2"]=="Manufacturing PMI")&(calendario["zone"]=="united kingdom"),"actual"]
        servicesPMI=calendario.loc[(calendario["event2"]=='Services PMI')&(calendario["zone"]=="united kingdom"),"actual"]
        industrialProductionMoM=calendario.loc[(calendario["event2"]=="Industrial Production MoM")&(calendario["zone"]=="united kingdom"),"actual"]
        industrialProductionYoY=calendario.loc[(calendario["event2"]=="Industrial Production YoY")&(calendario["zone"]=="united kingdom"),"actual"]
        cbiindustrialtrendorders=calendario.loc[(calendario["event2"]=='CBI Industrial Trends Orders')&(calendario["zone"]=="united kingdom"),"actual"]
        steelproduction=calendario.loc[(calendario["event2"]=='Steel Production')&(calendario["zone"]=="united kingdom"),"actual"]
        gdpyoy=calendario.loc[(calendario["event2"]=='GDP YoY')&(calendario["zone"]=="united kingdom"),"actual"]
        gdpqoq=calendario.loc[(calendario["event2"]=='GDP QoQ')&(calendario["zone"]=="united kingdom"),"actual"]
        ricshousebalance=calendario.loc[(calendario["event2"]=='RICS House Price Balance')&(calendario["zone"]=="united kingdom"),"actual"]
        ricshousebalance=calendario.loc[(calendario["event2"]=='Halifax House Price Index MoM')&(calendario["zone"]=="united kingdom"),"actual"]
        housepriceindexyoy=calendario.loc[(calendario["event2"]=='House Price Index YoY')&(calendario["zone"]=="united kingdom"),"actual"]
        halifaxhosuepriceindexyoy=calendario.loc[(calendario["event2"]=='Halifax House Price Index YoY')&(calendario["zone"]=="united kingdom"),"actual"]
        calendario2=pd.DataFrame(index=np.unique(calendario.index))
        calendario2["cpimom"]=cpimom[~cpimom.index.duplicated(keep='first')]
        calendario2["cpiyoy"]=cpiyoy[~cpiyoy.index.duplicated(keep='first')]
        calendario2["retailsalesyoy"]=retailsalesyoy[~retailsalesyoy.index.duplicated(keep='first')]
        calendario2["gfkcomsumerconfidence"]=gfkcomsumerconfidence[~gfkcomsumerconfidence.index.duplicated(keep='first')]
        calendario2["unemploymentrate"]=unemploymentrate[~unemploymentrate.index.duplicated(keep='first')]
        calendario2["manufacturingPMI"]=manufacturingPMI[~manufacturingPMI.index.duplicated(keep='first')]
        calendario2["industrialProductionMoM"]=industrialProductionMoM[~industrialProductionMoM.index.duplicated(keep='first')]
        calendario2["servicesPMI"]=servicesPMI[~servicesPMI.index.duplicated(keep='first')]
        calendario2["cbiindustrialtrendorders"]=cbiindustrialtrendorders[~cbiindustrialtrendorders.index.duplicated(keep='first')]
        #calendario2["steelproduction"]=steelproduction[~steelproduction.index.duplicated(keep='first')]
        calendario2["gdpyoy"]=gdpyoy[~gdpyoy.index.duplicated(keep='first')]
        calendario2["gdpqoq"]=gdpqoq[~gdpqoq.index.duplicated(keep='first')]
        calendario2["ricshousebalance"]=ricshousebalance[~ricshousebalance.index.duplicated(keep='first')]
        calendario2["ricshousebalance"]=ricshousebalance[~ricshousebalance.index.duplicated(keep='first')]
        calendario2["housepriceindexyoy"]=housepriceindexyoy[~housepriceindexyoy.index.duplicated(keep='first')]
        calendario2["halifaxhosuepriceindexyoy"]=halifaxhosuepriceindexyoy[~halifaxhosuepriceindexyoy.index.duplicated(keep='first')]
    if exchange=="PA":
        cpimom=calendario.loc[(calendario["event2"]=="French CPI MoM")&(calendario["zone"]=="france"),"actual"]
        cpiyoy=calendario.loc[(calendario["event2"]=="French CPI YoY")&(calendario["zone"]=="france"),"actual"]
        comsumerconfidence=calendario.loc[(calendario["event2"]=='French Consumer Confidence')&(calendario["zone"]=="france"),"actual"]
        unemploymentrate=calendario.loc[(calendario["event2"]== 'French Unemployment Rate')&(calendario["zone"]=="france"),"actual"]
        businessuervey=calendario.loc[(calendario["event2"]=="French Business Survey")&(calendario["zone"]=="france"),"actual"]
        manufacturingPMI=calendario.loc[(calendario["event2"]=="French Manufacturing PMI")&(calendario["zone"]=="france"),"actual"]
        servicesPMI=calendario.loc[(calendario["event2"]=='French Services PMI')&(calendario["zone"]=="france"),"actual"]
        industrialProductionMoM=calendario.loc[(calendario["event2"]=="French Industrial Production MoM")&(calendario["zone"]=="france"),"actual"]
        gdpyoy=calendario.loc[(calendario["event2"]=='French GDP YoY')&(calendario["zone"]=="france"),"actual"]
        gdpqoq=calendario.loc[(calendario["event2"]=='French GDP QoQ')&(calendario["zone"]=="france"),"actual"]
        housepriceindexqoq=calendario.loc[(calendario["event2"]=='French House Price Index QoQ')&(calendario["zone"]=="france"),"actual"]
        nonfarmpayrolls=calendario.loc[(calendario["event2"]=='French Non-Farm Payrolls QoQ')&(calendario["zone"]=="france"),"actual"]
        markitcompositepmi=calendario.loc[(calendario["event2"]=='French Markit Composite PMI')&(calendario["zone"]=="france"),"actual"]
        calendario2=pd.DataFrame(index=np.unique(calendario.index))
        calendario2["cpimom"]=cpimom[~cpimom.index.duplicated(keep='first')]
        #calendario2["cpiyoy"]=cpiyoy[~cpiyoy.index.duplicated(keep='first')]
        calendario2["businessuervey"]=businessuervey[~businessuervey.index.duplicated(keep='first')]
        calendario2["comsumerconfidence"]=comsumerconfidence[~comsumerconfidence.index.duplicated(keep='first')]
        #calendario2["unemploymentrate"]=unemploymentrate[~unemploymentrate.index.duplicated(keep='first')]
        calendario2["manufacturingPMI"]=manufacturingPMI[~manufacturingPMI.index.duplicated(keep='first')]
        calendario2["industrialProductionMoM"]=industrialProductionMoM[~industrialProductionMoM.index.duplicated(keep='first')]
        calendario2["servicesPMI"]=servicesPMI[~servicesPMI.index.duplicated(keep='first')]#
        #calendario2["gdpyoy"]=gdpyoy[~gdpyoy.index.duplicated(keep='first')]
        #calendario2["gdpqoq"]=gdpqoq[~gdpqoq.index.duplicated(keep='first')]
        #calendario2["housepriceindexqoq"]=housepriceindexqoq[~housepriceindexqoq.index.duplicated(keep='first')]
        calendario2["nonfarmpayrolls"]=nonfarmpayrolls[~nonfarmpayrolls.index.duplicated(keep='first')]
        #calendario2["markitcompositepmi"]=markitcompositepmi[~markitcompositepmi.index.duplicated(keep='first')]
        
                
    return calendario2
    


    