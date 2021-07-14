import logging
import time
from logging import handlers

import schedule
from strategy.buy import future_smooth_macd, stock_smooth_macd, future_smooth_macd_pre, stock_smooth_macd_pre

logger = logging.getLogger('/home/stock/app/time_friend/timed_task.log')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')

rf = handlers.RotatingFileHandler('./timed_task.log', encoding='UTF-8', maxBytes=1024, backupCount=0)
rf.setLevel(logging.INFO)
rf.setFormatter(formatter)

logger.addHandler(rf)


def stock_job():
    logger.info('starting stock_smooth_macd')
    try:
        stock_smooth_macd.start()
    except Exception as e:
        logger.error('error stock_smooth_macd, error = {0}'.format(str(e)))
    logger.info('finished stock_smooth_macd')

    logger.info('starting stock_smooth_macd_pre')
    try:
        stock_smooth_macd_pre.start()
    except Exception as e:
        logger.error('error stock_smooth_macd_pre, error = {0}'.format(str(e)))
    logger.info('finished stock_smooth_macd_pre')


def future_job():
    logger.info('starting future_smooth_macd')
    try:
        future_smooth_macd.start()
    except Exception as e:
        logger.error('error future_smooth_macd, error = {0}'.format(str(e)))
    logger.info('finished future_smooth_macd')

    logger.info('starting future_smooth_macd_pre')
    try:
        future_smooth_macd_pre.start()
    except Exception as e:
        logger.error('error future_smooth_macd_pre, error = {0}'.format(str(e)))
    logger.info('finished future_smooth_macd_pre')


def run():
    schedule.every().day.at("19:50").do(stock_job)
    schedule.every().day.at("21:30").do(future_job)


if __name__ == "__main__":
    run()
    while True:
        schedule.run_pending()
        time.sleep(1)
