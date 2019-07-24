# -*- coding: utf-8 -*-
import os
import sys

# Argumentos del programa en caso de que haya
args = sys.argv

# App directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
PATH = os.path.dirname(os.path.realpath(__file__)) + "/"

# App varaibles
# RETAILER=
RETAILER_KEY = os.getenv('RETAILER_KEY', 'rappi')
APP_NAME = os.getenv('APP_NAME', 'byprice-scraper-'+RETAILER_KEY)
SCRAPER_TYPE = os.getenv('SCRAPER_TYPE', 'price')
STORES = int(os.getenv('STORES', 1))

# Argumentos Servicio de geolocation
SRV_GEOLOCATION = os.getenv(
    'SRV_GEOLOCATION', 'gate.byprice.com/geo')  # Change this

# Data Streamer
STREAMER = os.getenv('STREAMER', 'rabbitmq')
STREAMER_HOST = os.getenv('STREAMER_HOST', 'rabbitmq')
STREAMER_PORT = os.getenv('STREAMER_PORT', '')

# Rabbit Streamer
if STREAMER == "rabbitmq":
    STREAMER_ROUTING_KEY = os.getenv('STREAMER_ROUTING_KEY', 'routing')
    STREAMER_EXCHANGE = os.getenv('STREAMER_EXCHANGE', 'data')
    STREAMER_EXCHANGE_TYPE = os.getenv('STREAMER_EXCHANGE_TYPE', 'direct')
    STREAMER_QUEUE = os.getenv('STREAMER_QUEUE', '')


# Celery Backend
CELERY_BROKER = os.getenv('CELERY_BROKER', 'rabbitmq')
CELERY_VIRTUAL_HOST = os.getenv('CELERY_VIRTUAL_HOST', "")
CELERY_USER = os.getenv('CELERY_USER', 'guest')
CELERY_PASS = os.getenv('CELERY_PASS', 'guest')
CELERY_PORT = os.getenv('CELERY_PORT', '5672')


# Env-dependent variables
ENV = os.getenv('ENV', 'DEV')
SRV_GEOLOCATION = 'dev.' + SRV_GEOLOCATION if ENV.upper() == 'DEV' else SRV_GEOLOCATION
QUEUE_ROUTING = "routing_dev" if ENV.upper() == 'DEV' else "routing"
CELERY_QUEUE = os.getenv('CELERY_QUEUE', RETAILER_KEY + '_'+ SCRAPER_TYPE)
CELERY_QUEUE = CELERY_QUEUE+"_dev" if (ENV.upper() == 'DEV' and 'dev' not in CELERY_QUEUE) else CELERY_QUEUE

STREAMER_ROUTING_KEY = STREAMER_ROUTING_KEY + '_dev' if os.getenv('ENV', 'DEV') == 'DEV' else STREAMER_ROUTING_KEY

# Logger
# Logging and remote logging
if ENV.upper() != 'LOCAL':
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_HOST = os.getenv('LOG_HOST', 'logs5.papertrailapp.com')
    LOG_PORT = os.getenv('LOG_PORT', 27971)
else:
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
    LOG_HOST = os.getenv('LOG_HOST', None)
    LOG_PORT = os.getenv('LOG_PORT', None)

OXYLABS = os.getenv('OXYLABS', None)