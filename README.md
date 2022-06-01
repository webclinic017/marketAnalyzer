# Market analyzer: forex and stocks

There are several tools to analyze financial data, visualize data and develop strategies using different portfolios based on analysis results. The intention of this proyect is not to be installed and used automatically (because it uses local storage and a lot of configurations so deployment is hard) but to see its different features, and if you want to use it please contact me at manuelpazpintor@gmail.com.
In this version  local storage is used but in next version cloud storege will be used.
## Structure
There proyect is mainly Python with the next structure:
* **index/**. Analayzer scripts (main).
* **functions/**. Analyzer auxiliary functions.
* **utils/**. Other functions.
* **reports/**. Plots and reports.
* **data/**. Data files in csv or json.
* **database/**. Databasee module.
* **config/**. Configuration files.
  * **utils/createEnvironment/**. Financial data is stored both using a MySQL database and csv files ( database to  do complex queries and csv to load quickly big amounts of data).The reason is that they are obtained using a payment API so there is a limit of queries. In this folder there are:
    * Scripts to create databases. 
    * script to run local database using Docker.
    * Scripts to create folder strucure to save *.csv*.
    * Script to get stocks that are traded in *Degiro* and *Admiral Markets* (in *.csv*, they must be imported to databases created before).
  * **utils/get_new_data/**. Get data from different APIs.
    * **utils/get_new_data/getAllData.sh**. Bash script to excute all the Python scripts and get data from all the sources (it should be executed daily).
* **logs/**. Log files and log conf
* **assets/superset**. Dashboards in *json* format.
## Deployment

Properties file called **config.properties** must be created and edited to load configurations in this folder, as shown in **example_config.properties**.

### Deploy:

```
cd createEnvironmment
run_container.sh
```
Then in the MYSQL instance:
```
createTable.sql
createTableInvesting.sql
```
Then:
```
cd degiro
python descargarStocksDEGIRO.py
cd ../admiralMarkets
python descargarStocksAdmirall.py (this must be done in Windows and then send the csv to the proyect in Windows because one library is only able in Windows)
cd ../initTables
python createDirStructure.py
pyhton saveSectorsAndGeneralInfo.py
```
Then you should export the DEGIRO and Admiral Markets csv to the MYSQL tables created with the names of the broker.
At this point you can use scripts in **getData/** folder to get data and explore tools in **analyzer/** folder to analyze data. You  can also install Apache Superset and use dashboards in **visualization/superset/** folder to visualiza macroeconomic data.
### Requirements

```
pip install -r requirements.txt
```

## Features ⚙️
 
* ARIMA, prophet and VAR models to predict prizes, macroeconomic info and fundamental results.
* Visualizations using Apache Superset.
* Logs in **logs/** dir.
* Databases with huge amounts of stocks prizes, forex, macroeconomic data from investig and stocks fundamental data (the first and the last one require payment in EOD)
* Portfolio and strategies creation based in analysis results.
## Build with 🛠️

* [Python]
* [Docker]
* [Apache Superser]
* [MLflow]



## Autors ✒️
Manuel Paz Pintor



---
⌨️ (https://github.com/ManuPaz) 😊
