from functions import metricas
import numpy as np 
import logging,logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')
def logs_metricas_prophet(forecast):
    logger.info("logs_prophet: METRICAS usando media {}".format(
        metricas.all_metricas(forecast.y, np.ones(len(forecast.y)) * np.mean(forecast.y))))
    logger.info("logs_prophet: METRICAS entrenamiento {}".format(metricas.all_metricas(forecast.y, forecast.yhat)))
    logger.info("logs_prophet: METRICAS tendencia entrenamiento {}".format(metricas.all_metricas(forecast.y, forecast.trend)))
    logger.info("logs_prophet: METRICAS estacioanalidad anual entrenamiento {}".format(
        metricas.all_metricas(forecast.y, forecast.yearly)))
    logger.info("logs_prophet: METRICAS terminos aditivos entrenamiento {}".format(
        metricas.all_metricas(forecast.y, forecast.additive_terms)))

    logger.info("logs_prophet: METRICAS sin usar estacionalidad anual en entrenamiento {}".format(
        metricas.all_metricas(forecast.y, forecast.yhat - forecast.yearly)))
    if "extra_regressors_additive" in forecast.columns:
        logger.info("logs_prophet: METRICAS  terminos aditivos extra regressors entrenamiento {}".format(
            metricas.all_metricas(forecast.y, forecast.extra_regressors_additive)))
