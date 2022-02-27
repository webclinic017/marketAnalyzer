use stocks;
show tables;
create table exchanges(
    code varchar(100) PRIMARY KEY,
    name varchar(100),
    currency varchar(100),
    operatingMIC varchar(100)

);
create table proximosResultados(
    stock varchar(100),
    exchange varchar(100),
    fecha date

);
#ALTER TABLE stocks CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
alter table exchanges add column country varchar(100)  after name;
alter table stocks drop column sector;
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
create table precios(
    stock varchar(100),
    exchange varchar(100),
    close double,
    open double,
    adjusted_close double,
    high double,
    low double,
    volume double,
    fecha date,
    PRIMARY KEY (stock,exchange,fecha)

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
create table lastPrizesUpdate(
    stock varchar (100),
    exchange varchar(100),
    fecha date,
    PRIMARY KEY (stock,exchange)


);
create table calendarioResultados(

    report_date date,
    date date,
    stock varchar(100),
    exchange varchar(100),
    actual double,
    estimate double

);
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


