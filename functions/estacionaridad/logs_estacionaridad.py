import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzig_data')
def log_stationarity_coef(resultadosEst):
    logging.info("logs_estacionaridad: Hurst: {}".format(str(resultadosEst["hurst"])))
    logging.info("logs_estacionaridad: Lambda adf: {}".format(resultadosEst["lambda_adf"]))
    logging.info("logs_estacionaridad: Test adf: {}".format(resultadosEst["results_adf"]))
