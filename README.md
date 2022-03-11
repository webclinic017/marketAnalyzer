# Market analyzer: forex and stocks

There are several tools to analyze financial data, visualize data and develop strategies using different portfolios based on analysis results. 
In this version  local storage is used but in next version cloud tools will be used.
##Structure
There are several tools that can be used:
**analyzer/**. Is the core folder where data is analyzed.
	**analyzer/mlflow**. Train and test models using MLflow
	**analyzer/getData**. Get data to train the models.
	**analyzer/Models**. Models to use (machine learninge, ARIMA and VAR)
	**anayzer/neuralNetworks**.
	**analyzer/notebooks**. Test the models before using it with MLFlow.
**createEnvironment/**. Financial data is stored both using a MYsql database and csv files ( database to  do complex queries and csv to load quickly big amounts of data).The reason is that they are obtained using a payment API so there is a limit of queries. In this folder there are:
	Scripts to create databases. 
	script to run local database using Docker.
	Scripts to create folder strucure to save csv.
	Script to get stocks that are traded in Degiro and Admiral Markets (in csv, they must be imported to databases created before).
**getData/**. Get data from different APIs.
	**getData/getAllData.sh**. Bash script to excute all the Python scripts and get data from all the sources (it should be executed daily).
Other folders are less important:
**writer/**. Show information via files or screen.
**logs/**. Log files.
**visualization/**. Visualization tools: matplotlib graphs and Apache Superset.
## Deployment
There are a lot of steps to deploy all the tools so if you want to do it please contact me:**manuelpazpintor@gmail.com**.
Anyway, if you want to try the doplyment by youself these are the steps:

Properties file called **config.properties** must be created to load configurations in this folder.
```
[DatabaseInvestingSection]
database_host=<host>
database_user=<user>
database_password=<password>
database_port=<port>
database_name=calendarios
database_table_name=calendarioEsperado
num_datos_antiguos_actualizar=<number of days back to check  Investing events>
[DatabaseForexSection]
database_host=<host>
database_user=<user>
database_password=<password>
database_port=<port>
database_name=forex
database_table_name=calendario
[OANDA_API_Section]
hostname={api-fxpractice.oanda.com} (api url, can be practice for demo or trade with real money)
port=443
token=df87c0f912c85c086f06620b0b910997-cfc6e55ef7879c6fe03f3c82131b7c23
cuenta=<account number of one of your oanda trade accounts>
ssl=True
datetime_format=UNIX
periodicidad={D} (period to get Data)
[DatabaseMacroSection]
database_host=<host>
database_user=<user>
database_password=<password>
database_port=<port>
database_name=calendarios
[DatabaseStocksSection]
database_host=<host>
database_user=<user>
database_password=<password>
database_port=<port>
database_name=stocks
database_name2=calendarios
database_table_name=stocks
storage_dir=<local folder to store prizes>
fundamental_storage_dir=<local folder to store fundamental data>
data_dir=<local folder to store exchanges and stocks information>
days_update=1
days_update_fundamental=40
days_update_results=10
days_check_fundamental=100
saveStockFundamentalsWhenUpdatingResults=True
[EOD_SECTION]
api_key=<EOD Historical Data API Key>
[DEGIRO_SECTION]
guia_dir=<folder to get data>
guardar_dir=<folder to save data> 
user=<DEGIRO user>
password=<DEGIRO password>
[ADMIRAL_MARKETS]
account_number=<Admiral Markets account number>
password=<Admiral Markets password>
guia_dir=<folder to get data>
guardar_dir=<folder to save data>
guia_dir_Windows=< folder to get Data in Windows version>
guardar_dir_WIndows=<folder to save Data in Windows version>
[DIR_STRUCTURE]
api_key=<EOD API key>
guia_dir=<folder to get info>
storage_dir=<folder to store prizes>
database_host=<host>
database_user=<user>
database_password=<password>
database_port=<port>
database_name=stocks
[LOGS_DIR]
rendimiento=logs/rendimiento.txt
excepciones=logs/excepciones.txt
[PERFORMANCE]
time=True
[Entrenamiento]
tam_train=0.7
classification_metric=f1
cv=3
scale=True
[EntrenamientoARIMA]
periodicidad=4
max_lag=5
nivel_confianza=0.1
tam_train=0.8
```
Deploy:

```
cd createEnvironmment
run_container.sh
```
Then in the MYSQL instance:
```
createTable.sql
createTableInvestin.sql
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
```
At this point you can use scripts in **getData/** folder to get data and explore tools in **analyzer/** folder to analyze data. You  can also install Apache Superset and use dashboards in **visualization/superset/** folder to visualiza macroeconomic data.
### Requirements

```
pip install -r requirements.txt
```

## Features ⚙️
 
* ARIMA and VAR models to predict prizes, macroeconomic info and fundamental results.
* Machine learninge models to predict prizes, macroeconomic data and fundamental results.
* Visualizations using Apache Superset and Jupyter Notebooks.
* Jupyter Notebooks explaing some traings.
* Logs in logs/ dir
* Databases with huge amounts of stocks prizes, forex, macroeconomic data from investig and stocks fundamental data (the first and the last one require payment in EOD)
* Portfolio and strategies creation based in analysis results
## Build with 🛠️

* [Python]
* [Docker]
* [Apache Superser]
* [MLflow]



## Autors ✒️
Manuel Paz Pintor



---
⌨️ (https://github.com/ManuPaz) 😊
