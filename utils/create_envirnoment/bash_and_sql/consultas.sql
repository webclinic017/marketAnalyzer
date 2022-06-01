
show tables;
--INVESTING EVENTS
select distinct(event) from calendarioEsperado where zone='united states';
select distinct(event) from calendarioEsperado where zone='united kingdom';
select distinct(event) from calendarioEsperado where zone='spain';


-------------------------------------------------------------------------------------
--INVESTING EVENTS
select distinct(zone) from calendarioEsperado order by zone  asc;
select * from calendarioEsperado where zone='united states' and event like '%%';
select * from calendarioEsperado where zone='united states' and event like '%Domestic Car Sales%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%Factory orders (MOM)%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%ISM Non-Manufacturing PMI%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%ISM Manufacturing PMI%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%Core CPI (MoM)%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%Core CPI (yoy)%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%Industrial Production (MoM)%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%GDP (QoQ)%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%Building Permits  (%)' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like '%Fed Interest Rate Decision%' order by fecha desc;
select * from calendarioEsperado where zone='united states' and event like 'Unemployment Rate%' order by fecha desc;
-------------------------------------------------------
select * from calendarioEsperado where zone='united kingdom' and event like '%BoE Interest Rate Decision%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Manufacturing PMI%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Core Retail Sales (MoM)%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Core Retail Sales (YoY)%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%CPI (MoM)%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Core CPI (YoY)%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Industrial Production (MoM)%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Industrial Production (YoY)%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%Unemployment Rate%' order by fecha desc;
select * from calendarioEsperado where zone='united kingdom' and event like '%GDP (QoQ)%' order by fecha desc;
-------------------------------------------------------
select * from calendarioEsperado where zone='spain' and event like '%Spanish CPI (YoY)%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish CPI (MoM)%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish Industrial Production (YoY)%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish Retail Sales (YoY)%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish Unemployment Rate%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish Manufacturing PMI%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish Services PMI%' order by fecha desc;
select * from calendarioEsperado where zone='spain' and event like '%Spanish GDP (QoQ)%' order by fecha desc;

-------------------------------------------------------

select * from indices WHERE INDICE='NASDAQ';
select * from indices;
select * from preciosindices where indice='sp500';
select * from preciosindices where indice='dow';
-------------------------------------------------------

--OTHER EQUITIES
SHOW TABLES;
select distinct(name) from commodities;
delete from commodities where name not in ('gold','silver','crude Oil WTI','cooper')
select time as fecha,close from EUR_USD order by fecha asc;
select fecha,close from commodities where name='gold' order by fecha desc;
select * from bonds order by fecha desc;
select * from EUR_USD order by date desc;
SELECT * FROM bonds;
delete  from bonds WHERE name not like '%10Y%'
                    and name not like '%30Y%'
                    and name not like '%3Y%'
                    and name not like '%2Y%'
                    and name not like '%1Y%'
                    and name not like '%7Y%'
                    and name not like '%5Y%'
                    and name not like '%3M%'
                    and name not like '%1M%'
                    and name not like '%6M%';
-------------------------------------------------------
--RATIO RESULTS (PROCESSED TABLES)
select * from   ratios_results as rr  where adjusted_close is not null  and fecha>= all(select fecha from ratios_results where adjusted_close is not null and stock=rr.stock and exchange=rr.exchange)
select stocks.name,indices.indice,ca.stock,ca.exchange, h.MarketCapitalization,ca.actual,ca.estimate,ca.report_date,ca.date, h.ebitda,peratio, bookvalue,h.QuarterlyEarningsGrowthYOY,h.QuarterlyRevenueGrowthYOY,sector from (select * from calendarioResultados where  exchange='US' and report_date>DATE_ADD(now(),interval -60 day ))  ca
    inner join indices on ca.stock=indices.company
    inner join  stocks on stocks.code=ca.stock
    inner join last_highlights as h on h.stock=ca.stock and h.exchange=ca.exchange
    inner join sectors on sectors.stock=ca.stock and sectors.exchange=ca.exchange
where indices.indice='SP500' and ca.report_date<now() AND (STOCKS.exchange='NYSE' or stocks.exchange='nasdaq')
order by report_date desc;








