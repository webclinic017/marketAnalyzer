import os
os.chdir("../../")
from config import load_config
config = load_config.config()
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
import datetime as dt
from utils.database import bd_handler, database_functions
from utils.get_new_data import update_stock
from dateutil.relativedelta import relativedelta
from plots.plot_specific_series import plot_specific_series
import pandas as pd
fecha_corte_plots = "2010-01-01"
indice = "nasdaq"
exchange = "US"
update = False
fecha_ini = dt.datetime.today() - relativedelta(years=15)
base_dir = config["analyze_groups_stocks"]["base_dir"]
if not os.path.isdir(base_dir):
    os.mkdir(base_dir)
base_dir = base_dir + indice + "/"
if not os.path.isdir(base_dir):
    os.mkdir(base_dir)
for e in ["tablas", "plots", "stocks"]:
    if not os.path.isdir(base_dir + e):
        os.mkdir(base_dir + e)
if __name__ == "__main__":
    hoy = dt.date.today()
    fechas = [hoy - relativedelta(months=1), hoy - relativedelta(months=3), hoy.replace(month=1, day=1),
              hoy - relativedelta(years=1), hoy - relativedelta(years=2)]
    bd = bd_handler.bd_handler("stocks")
    stocks = database_functions.filter_by_indice(indice, exchange, bd)
    stocks = stocks.accion.values.reshape(-1)
    if update:
        for stock in stocks:
            stock = stock.split("_")[1]
            update_stock.update_one_stock_in_all_tables(exchange, stock, bd)

    data = database_functions.obtener_multiples_series("precios",
                                                       "B", *stocks, bd=bd)

    data = data.loc[data.index > dt.datetime.strptime(config["fecha_ini_default"], "%Y-%m-%d")]

    data = data.dropna()
    len0 = data.shape[0]
    logger.info("indice.py: Len of {} data:  {}".format(indice, len0))

    df_changes = pd.DataFrame(index=stocks, columns=[str((hoy - e).days) for e in fechas])

    for fecha in fechas:
        fecha = dt.datetime(fecha.year, fecha.month, fecha.day)

        df_aux = (data.iloc[-1] - data.loc[data.index > fecha].head(1)) / data.loc[data.index > fecha].head(1)
        df_changes[str((hoy - fecha.date()).days)] = round(df_aux.transpose(), 3)

    descriptions_sql = "select concat(sectors.exchange,'_',sectors.stock) as stock, description, sector from (select * from descriptions where " \
                       "exchange=%s) as d  inner join (select * from indices where indice=%s) as i" \
                       " on d.stock=i.company inner join sectors on sectors.stock=d.stock where sectors.exchange=%s "
    df_decriptions = bd.execute_query_dataframe(descriptions_sql, (exchange, indice, exchange))
    df_decriptions.set_index("stock", inplace=True)

    df_changes["description"] = df_decriptions.description
    df_changes["sector"] = df_decriptions.sector
    df_changes.sort_values(by=str((hoy - fechas[2]).days), inplace=True)
    df_changes.to_csv(base_dir + "tablas/" + "pct_changes.csv")

    query_next_results = "select report_date,date,stock from (select * from calendarioResultados where exchange=%s and report_date>%s) as e inner  join (select * from indices where indice=%s) as n " \
                         "on e.stock=n.company order by report_date desc;"
    df_next_results = bd.execute_query_dataframe(query_next_results, (exchange, dt.datetime.today(), indice))
    df_next_results.to_csv(base_dir + "tablas/" + "next_results.csv")

    query_last_highlights = "select fecha, stock, MarketCapitalizationMln, ebitda,peratio, EPSEstimateCurrentQuarter,EPSEstimateCurrentYear,EPSEstimateNextQuarter,EPSEstimateNextYear," \
                            "QuarterlyEarningsGrowthYOY,QuarterlyRevenueGrowthYOY " \
                            "from (select * from last_highlights where  exchange=%s ) as e inner  join (select * from indices where indice=%s) as n on e.stock=n.company;"

    df_last_highlights = bd.execute_query_dataframe(query_last_highlights, (exchange, indice))
    df_last_highlights["pct_change_ESTIMATE_EPS"] = round((
                                                                  df_last_highlights.EPSEstimateNextYear - df_last_highlights.EPSEstimateCurrentYear) / df_last_highlights.EPSEstimateCurrentYear,
                                                          3)
    df_last_highlights["stock"] = exchange + "_" + df_last_highlights.stock
    df_last_highlights.set_index("stock", inplace=True)
    df_last_highlights["pct_change"] = df_changes[str((hoy - fechas[2]).days)]

    df_last_highlights.to_csv(base_dir + "tablas/" + "last_highlights.csv")
    for stock in df_changes.index:
        code = stock
        stock = stock.split("_")[1]
        directorio = base_dir + "stocks/" + stock
        if not os.path.isdir(directorio):
            os.mkdir(directorio)

        nombre_serie = ["netIncome", "totalrevenue", "totalAssets", "totalLiab"]
        # nombre_serie="netIncome"
        # nombre_serie="adjusted_close"
        freq = "D"
        plot_specific_series(nombre_serie, code, freq, fecha_corte_plots, bd,
                             file_name=base_dir + "stocks/" + stock + "/fundamental.jpg")
        sql = "select report_date, fecha, Adjusted_close, netIncome, totalrevenue/1000000 as totalrevenue from ratios_results where stock=%s and exchange=%s and report_date>%s " \
              "order by report_date desc; "
        df_stock = bd.execute_query_dataframe(sql, (stock, exchange, fecha_ini))
        try:
            df_stock.to_csv(base_dir + "stocks/" + stock + "/historical.csv")
        except AttributeError as e:
            logger.error("index.py: {}".format(e))
