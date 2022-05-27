use stocks;
show tables;
create table exchanges(
    code varchar(100) PRIMARY KEY,
    name varchar(100),
    country varchar(100),
    currency varchar(100),
    operatingMIC varchar(100)

);
--las otras tabla se crean desde python: forex, precios y fundamentals (se crean primero las de precios y sobre ellas las de fundamentals)



--la siguiente linea es necesaria
--ALTER TABLE stocks CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
create table stocks(
    code varchar(100),
    name varchar(1000),
    exchange varchar(100),
    isin varchar(100),
    type varchar(100),
    country varchar(100),
    Currency varchar(100),
    PRIMARY KEY (code,exchange)


);
create table degiro(
    code varchar(100),
    name varchar(1000),
    exchange varchar(100),
    isin varchar(100),
    type varchar(100),
    country varchar(100),
    Currency varchar(100),
    PRIMARY KEY (code,exchange)


);
create table admiralmarkets(
    code varchar(100),
    name varchar(1000),
    exchange varchar(100),
    isin varchar(100),
    type varchar(100),
    country varchar(100),
    Currency varchar(100),
    PRIMARY KEY (code,exchange)


);
create table indices(
    company varchar(1000),
    indice varchar(100)
);
create table sectors(
  stock varchar(100),
  exchange varchar(100),
  sector varchar(1000)

);
create table descriptions(
  stock varchar(100),
  exchange varchar(100),
  description varchar(10000)

);
create table calendarioResultados(

    report_date date,
    date date,
    stock varchar(100),
    exchange varchar(100),
    actual double,
    estimate double

);
-- auxiliar, datos procesados
CREATE table preciosPorSectores(
exchange varchar(100),
sector varchar(100),
fecha datetime,
precio double

);

-- auxiliar, datos procesados
create table ratios_results(
    stock varchar(100),
    exchange varchar(100),
    fecha date,
    netIncome double,
    totalAssets double,
    totalLiab double,
    totalCurrentLiabilities double,
    totalStockholderEquity double,
    commonStockSharesOutstanding double,
    netDebt double,
    freeCashFlow double,
    totalRevenue double,
    ebit double,
    ebitda double,
    adjusted_close double,
    per double,
    earnings double,
    report_date date,
    earnings_per double,
    prizebook double,
    solvenciaLargo double,
    solvenciaCorto double,
    liquidez double
        );

create table exchangeIndice(
    exchange varchar(100),
    indice varchar(100)

);
insert into exchangeIndice values('US','DOW'),
                                  ('US','NASDAQ100'),
                                  ('PA','CAC40'),
                                  ('TO','TSX60'),
                                  ('US','SP500'),
                                  ('MC','IBEX35'),
                                  ('XETRA','DAX'),
                                  ('LSE','FTSE100Index'),
                                  ('MI','FTSEMIB');
                                  
--esta tabla se crea en Python pero se modifica asi
ALTER TABLE highlights
MODIFY  MostRecentQuarter date;


--ya no se usa
create table updated(
    fecha date,
    stock varchar(100),
    exchange varchar(100));
insert into indice_exchange values('SP500','US');
insert into indice_exchange values('NASDAQ100','US');
insert into indice_exchange values('DAX','XETRA');
insert into indice_exchange values('DAX','DU');
insert into indice_exchange values('DAX','BE');
insert into indice_exchange values('DAX','F');
insert into indice_exchange values('DAX','HA');
insert into indice_exchange values('IBEX35','US');
insert into indice_exchange values('FTSEMIB','MI');
insert into indice_exchange values('FTSE100Index','LSE');
insert into indice_exchange values('CAC40','PA');
insert into indice_exchange values('TSX60','NEO');
insert into indice_exchange values('TSX60','TO');
insert into indice_exchange values('TSX60','VX');
--ya no se usa
create table proximosResultados(
    stock varchar(100),
    exchange varchar(100),
    fecha date

);
--ya no se usa
create table lastPrizesUpdate(
    stock varchar (100),
    exchange varchar(100),
    fecha date,
    PRIMARY KEY (stock,exchange)


);





