use stocks;
show tables;
create table exchanges(
    code varchar(100) PRIMARY KEY,
    name varchar(100),
    country varchar(100),
    currency varchar(100),
    operatingMIC varchar(100)

);
--las otras tabla se crean desde python: todas para otros activos, precios y fundamentalsls)



--la siguiente linea es necesaria
--ALTER TABLE stocks CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
create table if not exists stocks(
    code varchar(100),
    name varchar(1000),
    exchange varchar(100),
    isin varchar(100),
    type varchar(100),
    country varchar(100),
    Currency varchar(100),
    PRIMARY KEY (code,exchange)


);
create table if not exists degiro(
    code varchar(100),
    name varchar(1000),
    exchange varchar(100),
    isin varchar(100),
    type varchar(100),
    country varchar(100),
    Currency varchar(100),
    PRIMARY KEY (code,exchange)


);
create table if not exists admiralmarkets(
    code varchar(100),
    name varchar(1000),
    exchange varchar(100),
    isin varchar(100),
    type varchar(100),
    country varchar(100),
    Currency varchar(100),
    PRIMARY KEY (code,exchange)


);
create table  if not exists indices(
    company varchar(1000),
    indice varchar(100)
);
create table if not exists sectors(
  stock varchar(100),
  exchange varchar(100),
  sector varchar(1000)

);
create table if not exists descriptions(
  stock varchar(100),
  exchange varchar(100),
  description varchar(10000)

);
create table if not exists calendarioResultados(

    report_date date,
    date date,
    stock varchar(100),
    exchange varchar(100),
    actual double,
    estimate double

);

-- auxiliar, datos procesados
create table if not exists ratios_results(
    stock varchar(100),
    exchange varchar(100),
    fecha date,
    netIncome double,
    totalAssets double,
    totalLiab double,
    totalCurrentLiabilities double,
    totalStockholderEquity double,
    commonStockSharesOutstanding_last double,
    commonStockSharesOutstanding double,
    netDebt double,
    freeCashFlow double,
    totalRevenue double,
    ebit double,
    ebitda double,
    adjusted_close double,
    per double,
    earnings double,
    adjusted_earnings double,
    report_date date,
    prizebook double,
    solvenciaLargo double,
    solvenciaCorto double,
    liquidez double
        );

--esimations from web pages
create table IF NOT EXISTS  oficial_estimations(
    fecha date,
    netIncome double,
    earnings double,
    totalrevenue double,
    stock double,
    exchange double
)



--splits
create  table if not exists splits(
    exchange varchar(100),
    stock varchar(100),
    optionable varchar(100),
    old_shares int,
    new_shares int,
    updatedOtherTables int,
    split_date date
);

                                  
--esta tabla se crea en Python pero se modifica asi
ALTER TABLE highlights
MODIFY  MostRecentQuarter date;




