# -*- coding: utf-8 -*-
import json
import os
import random
import sys

from ByHelpers import applogger
from ByRequests.ByRequests import ByRequest
from ByHelpers.rabbit_engine import stream_monitor

from config import *
from worker import crawl_store, start_stores, logger

br = ByRequest(attempts=2)

# URLS
geoloc_host = 'http://' + str(SRV_GEOLOCATION)
stores_endp_url = geoloc_host +'/store/retailer?key=%s'
base_url = "https://super.walmart.com.mx"


#Variables
retailer_key = 'rappi'
retailer_name = 'Rappi'


def call_scraper(params, ms_id):
    """ Call to crawl async elements

        Params:
        -------
        params: dict
            Stores params to crawl
        ms_id: uuid
            Master scraper ID 
    """
    logger.debug("Calling to scrape store...")
    crawl_store.apply_async(args=(ms_id, params), queue=CELERY_QUEUE)


def call_stores(ms_id, st_id):
    """ Call to crawl async stores 

        Params:
        -----
        ms_id: uuid 
            Master scraper ID
        st_id: uuid
            Store uuids of past stores
    """
    start_stores.apply_async(args=(ms_id, st_id), queue=CELERY_QUEUE)


def request_valid_stores(rt_key):
    """ Method that requests valid stores to geolocation endpoint

        Params:
        -----
        rt_key : str
            Routing Key (store, item, price)
    """
    # Fetch stores info from Geolocation Service
    r = br.get(stores_endp_url%retailer_key)
    if not r:
        print('Request error to geolocation service!')
        return
    stores_d = r.json()
    stores_list = [
        {
            'route_key' : rt_key.lower(),
            'retailer_key': 'rappi',
            'external_id' : st['external_id'],
            'store_uuid'  : st['uuid'],
            'name'        : st['name']
        } for st in stores_d
    ]
    return stores_list[: int(STORES)]



# Main method
if __name__ == '__main__':
    ms_id = stream_monitor('master', params={})
    try:
        logger.info("Started master scraper: " + CELERY_QUEUE + " / scraper_type: "+str(SCRAPER_TYPE))
        if SCRAPER_TYPE and len(SCRAPER_TYPE) > 0:
            if SCRAPER_TYPE == 'price' or SCRAPER_TYPE == 'item':
                # Fetch Valid Stores
                sts_to_crawl = request_valid_stores(str(SCRAPER_TYPE))
                logger.debug(sts_to_crawl[0])
                # Number of stores to crawl
                num_stores = range(0, len(sts_to_crawl))
                ms_id = stream_monitor('master', params=sts_to_crawl[0], num_stores=len(sts_to_crawl))
                logger.info("Crawling {} stores!".format(STORES if len(sts_to_crawl) > int(STORES) else len(sts_to_crawl)))
                # Call to crawl all stores async
                for s in num_stores:
                    logger.debug("Calling to scrape")
                    call_scraper(sts_to_crawl[s], ms_id)
                    # call_parallel(sts_to_crawl[s], ms_id)
            elif SCRAPER_TYPE == 'store':
                logger.debug("CALLING STORES")
                ms_id = stream_monitor('master', params={})
                st_id = 1
                call_stores(ms_id, st_id)
            else:
                logger.warning('Please indicate the argument type of scraping process')
    except Exception as e:
        error = "Error while executing master: {}".format(e)
        logger.error(e)
        ms_id = stream_monitor('master', params={})
        es_id = stream_monitor('error', ms_id=ms_id, code=2, reason=e)
