# -*- coding: utf-8 -*-
import datetime
from pprint import pprint

import pandas as pd
import celery
from ByRequests.ByRequests import ByRequest
from celery import Celery

from ByHelpers import applogger
from ByHelpers.rabbit_engine import (MonitorException, stream_info,
                                     stream_monitor)
from config import *
# from app.get_stores import get_stores, get_stores_from_coords
# from config import CELERY_QUEUE, OXYLABS, SRV_GEOLOCATION

# Avoid Celery extra logging
@celery.signals.setup_logging.connect
def on_setup_logging(**kwargs):
    pass

# logging
applogger.create_logger()
logger = applogger.get_logger()


br = ByRequest(attempts=1)
br.add_proxy(OXYLABS, attempts=2, name="Oxylabs")  

stores_dict = {
    'la_comer': {
        'name': 'La Comer',
        'key': 'rappi_la_comer'
    },
    'costco': {
        'name': 'Costco', 
        'key': 'rappi_costco'
    },
    'chedraui': {
        'name': 'Chedraui',
        'key': 'rappi_chedraui'
    },
    'city_market': {
        'name': 'City Market',
        'key': 'rappi_city_market'
    },
    'heb': {
        'name': 'HEB',
        'key': 'rappi_heb'
    },
    'fresko': {
        'name': 'Fresko',
        'key': 'rappi_fresko'
    },
    'san_pablo': {
        'name': 'San Pablo',
        'key': 'rappi_san_pablo'
    },
    'soriana': {
        'name': 'Soriana',
        'key': 'rappi_soriana'
    },
    'walmart': {
        'name': 'Walmart', 
        'key': 'rappi_walmart'
    },
    'benavides': {
        'name': 'Benavides',
        'key': 'rappi_benavides'
    },
    'farmacias_similares': {
        'name': 'Farmacias Similares',
        'key': 'rappi_farmacias_similares'
    },
    'la_europea': {
        'name': 'La Europea',
        'key': 'rappi_la_europea'
    },
    'superama': {
        'name': 'Superama', 
        'key': 'rappi_superama'
    },
    'f_ahorro': {
        'name': 'Del Ahorro',
        'key': 'rappi_f_ahorro'
    },
    'farmatodo': {
        'name': 'Farmatodo',
        'key': 'rappi_farmatodo'
    },
    'farmazone': {
        'name': 'Farma Zone',
        'key': 'rappi_farmazone'
    },
    'farmaciasguadalajara': {
        'name': 'Farmacias Guadalajara',
        'key': 'rappi_farmaciasguadalajara'
    }
}


# Variables
RETAILER = 'rappi'
base_url = 'https://www.rappi.com.mx/'
url_store = 'https://services.mxgrability.rappi.com/windu/corridors/sub_corridors/store/{}'
url_cat = 'https://services.mxgrability.rappi.com/api/subcorridor_sections/products?subcorridor_id={}&store_id={}&include_stock_out=true&limit={}'
url_image = 'https://images.rappi.com.mx/products/{}'
url_zip = "http://" + SRV_GEOLOCATION + "/place/get_places?zip={}"
url_product = base_url + 'product/{}'

LIMIT = 200

broker = 'amqp://{}:{}@{}:{}/{}'.format(
                CELERY_USER, CELERY_PASS, CELERY_BROKER, CELERY_PORT, CELERY_VIRTUAL_HOST)

queue = CELERY_QUEUE


# Celery app initilaization
app = Celery('worker', broker=broker)
app.conf.task_routes = {'worker.*': {'queue': queue}}
app.conf.enable_utc = True
app.conf.CELERY_ACCEPT_CONTENT = ['json']
app.conf.CELERY_RESULT_SERIALIZER = 'json'
app.conf.CELERY_TASK_SERIALIZER = 'json'

# app.conf.update({
#     'worker_hijack_root_logger': False, # so celery does not set up its loggers
#     'worker_redirect_stdouts': False, # so celery does not redirect its logs
# })


@app.task
def start_stores(master_id, st_id):
    monitor_dict = { 
        'ms_id'   : master_id,
        'store_id': st_id,
    }
    return get_stores(monitor_dict)


def get_zip():
    try:
        z_list = list(pd.read_csv('files/zips.csv')['zip'])
    except:
        z_list = ['03400', '01000','44100','64000']#,'76000','50000']
    return z_list


def get_stores(params):
    errors = []
    br_stats = {}
    try:
        # Obtain Rappi stores for each ZIP
        for zip_code in get_zip():
            process_zip.apply_async(args=(zip_code,), queue=CELERY_QUEUE)

        if len(errors) > 0:
            ws_id = stream_monitor('worker', step='store', ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
            for error in errors:
                stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=error.code, reason=str(error.reason))
        else:
            stream_monitor('worker', step='store', ms_id=params['ms_id'], store_id=params['store_id'])

    except Exception as e:
        ws_id = stream_monitor('worker', step='store', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=str(e))
        logger.error("Error in get_stores: " + str(e))
    return True


@app.task
def process_zip(zip_code):
    logger.debug('[ByRequests] Requesting {}'.format(url_zip.format(zip_code)))
    response = br.get(url_zip.format(zip_code), return_json=True)
    br_stats = br.stats

    if isinstance(response, dict):
        logger.debug('Resp is dict')
        places = response.get('places', [])

        logger.debug(places)
        
        for place in places:
            gral_data = {
                        "state" : place.get('state'),
                        "country" : "México",
                        "city" : place.get('city'),
                        "zip" : place.get('zip')
                    }

            get_stores_from_coords.apply_async(args=(place['lat'], place['lng'], gral_data), queue=CELERY_QUEUE)
            get_stores_from_coords(place['lat'], place['lng'], gral_data)
    else:
        err_st = 'Could not get right response from {}'.format(url_zip.format(zip_code))
        errors.append(MonitorException(code=2, reason=err_st))
        logger.error(err_st)
    return True


def get_ret_key(name):
    key = 'rappi'
    logger.debug('Store name: {}'.format(name))
    for k, d in stores_dict.items():
        if str(d['name']).lower() in name.lower():
            key = d['key']
    return key


def create_st_dict(loc, nm):
    st_id = loc.get('store_id')
    if st_id is not None:
        st_dict = {
            "route_key" : "store",
            "retailer" : get_ret_key(nm),
            "name" : nm,
            "external_id" : st_id,
            "address" : "",
            "street" : "",
            "phone" : "",
            "date" : str(datetime.datetime.utcnow()),
            "checked" : 0,
            "active" : 1,
            "online" : 1,
            "coords" : {
                "lat" : float(loc['lat']),
                "lng" : float(loc['lng'])
            }
        }
        return st_dict
    else:
        logger.error('Missing store_id')
        return None


@app.task
def get_stores_from_coords(lat, lng, gral_data={}):
    url_coord = "https://services.mxgrability.rappi.com/api/base-crack/principal?lat={}&lng={}&device=2"
    br = ByRequest(attempts=2)
    br.add_proxy(OXYLABS, attempts=5, name="Oxylabs")
    stores_ls = []
    lat = frmt_coord(lat)
    lng = frmt_coord(lng)
    logger.debug('[ByRequest] Requesting {}'.format(url_coord.format(lat, lng)))
    resp = br.get(url_coord.format(lat, lng), return_json=True)
    if isinstance(resp, list):
        logger.debug('Got response')
        # pprint(resp)
        stores_ls = extract_stores(resp)
    else:
        logger.error('Not a valid response, check if the site changed')
    for raw_st in stores_ls:
        try:
            for loc in raw_st.get('locations', []):
                clean_store = create_st_dict(loc, raw_st.get('name'))
                if isinstance(clean_store, dict):
                    if clean_store['retailer'] != 'rappi':
                        clean_store.update(gral_data)
                        logger.debug(clean_store)
                        stream_info(clean_store)
        except Exception as ex:
            err_st = 'Error with store {}'.format(raw_st)
            errors.append(MonitorException(code=3, reason=err_st))
            logger.error(err_st)

    logger.info('Found {} stores'.format(len(stores_ls)))
    return stores_ls


def frmt_coord(crd):
    if not isinstance(crd, float):
        try:
            crd = float(crd)
            crd = round(crd, 3)
        except Exception as e:
            logger.error('Could not convert lat to float')
    return crd


def extract_stores(st_raw):
    stores_list = []
    for st_el in st_raw:
        suboptions = st_el.get('suboptions', [])
        if len(suboptions) > 0:
            for store in suboptions:
                loc_list = []
                locations = store.get('stores', [])
                if len(locations) > 0:
                    lat = None
                    lng = None
                    st_id = None
                    for loc in locations:
                        l_dir = {
                            'lat'      : loc.get('lat'),
                            'lng'      : loc.get('lng'),
                            'store_id' : loc.get('store_id')
                        }
                        if l_dir['store_id'] is not None:
                            loc_list.append(l_dir)
                st_dict = {
                    'name' : 'Rappi ' + store.get('name'),
                    'type' : store.get('sub_group'),
                    'locations' : loc_list
                }
                if len(st_dict['locations']) > 0 and str(st_dict['type']).lower() in ['super', 'licores', 'farmacia']:
                    stores_list.append(st_dict)
        else:
            logger.debug('Got no suboptions: {}'.format(st_el.get('name')))
    return stores_list


@app.task
def crawl_store(master_id, params):
    monitor_dict = { 
        'ms_id'   : master_id,
        'store_id': params['store_uuid'],
    }
    params.update(monitor_dict)
    logger.debug(params)
    all_deps = get_store_deps(params)
    for dep in all_deps:
        scount = 0
        for scat in dep['sub_dep']:
            scount += 1
            # crawl_cat(dep['name'], scat, params)
            crawl_cat.apply_async(args=(dep['name'], scat, params), queue=CELERY_QUEUE)
        logger.info('{} Subcategories in {}'.format(scount, dep['name']))
    logger.info('Found {} departments in store {}'.format(len(all_deps), params['name']))
    return all_deps



def get_store_deps(params):
    """
        Method to extract the departments in retailer
    """
    dep_list = []
    br_stats = {}
    try:
        store_id = params['external_id']
        # Prepare request
        br = ByRequest(attempts=1)
        br.add_proxy(OXYLABS, attempts=3, name='Oxylabs')
        logger.debug('[ByRequest] Rquesting {}'.format(url_store.format(store_id)))
        response = br.get(url_store.format(store_id), return_json=True)
        br_stats = br.stats
        ws_id = stream_monitor('worker', step='start', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)

        if response:
            # Add departments
            for dep in response:
                dep_list.append(extract_info(dep))

        else:
            err_st = 'Could not get response for {}'.format(url_store.format(store_id))
            logger.error(err_st)
            stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=0, reason=str(err_st))
                
    except Exception as e:
        err_st = "Unexpected error in get_store_deps: {}".format(e)
        ws_id = stream_monitor('worker', step='start', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=err_st)
        logger.error(err_st)
        logger.debug(params)
    return dep_list



def extract_info(raw_dep_dict):
    sub_cat_ls = raw_dep_dict.get('sub_corridors', [])
    sdep = [extract_info(scat) for scat in sub_cat_ls]
    n_dict = {
        'name'   : raw_dep_dict.get('name'),
        'id'     : raw_dep_dict.get('id')
    }
    if len(sdep) > 0:
        n_dict.update({
            'sub_dep' : sdep
        })
    return n_dict


@app.task
def crawl_cat(dep_name, scat, params, page=1, next_id=None, run_all=True):
    br_stats = {}
    br = ByRequest(attempts=1)
    br.add_proxy(OXYLABS, attempts=3, name='Oxylabs')
    errors = []

    # Url creation
    url = url_cat.format(scat['id'], params['external_id'], LIMIT)
    if next_id is not None:
        url = url + '&next_id={}'.format(next_id)

    logger.debug('[ByRequest] Requesting {}'.format(url))
    
    try:
        response = br.get(url, return_json=True)
        br_stats = br.stats
        next_id = None
        prod_raw_ls = []
        prods_ls = []
        cat_ls = [dep_name, scat['name']]

        # Product list extraction
        if isinstance(response, dict):
            next_id = response.get('next_id')
            result = response.get('results', [])
            for res in result:
                prod_raw_ls.extend(res.get('products', []))
        else:
            err_st = 'Could not get response from {}'.format(url)
            logger.error(err_st)
            errors.append(MonitorException(code=0, reason=err_st))

        # Check if there are more products to crawl
        n_prod = len(prod_raw_ls)
        logger.info('Found {} products, page {} for {} | {}'.format(str(n_prod).ljust(3), str(page).ljust(2), params['retailer_key'], ' | '.join(cat_ls)))

        if (next_id is not None) and run_all:
            logger.debug('Found next page...')
            # crawl_cat(dep_name, scat, params, page=page+1, next_id=next_id)
            crawl_cat.apply_async(args=(dep_name, scat, params, page+1, next_id), queue=CELERY_QUEUE)
        for prod in prod_raw_ls:
            try:
                prod_clean = process_prod(prod, params)
                if prod_clean:
                    prod_clean.update({
                        'categories': cat_ls,
                    })
                    prods_ls.append(prod_clean)
                    stream_info(prod_clean)
                else:
                    err = 'Could not get product'
                    logger.error(err)
                    raise Exception(err)
            except Exception as exe:
                err_st = 'Error with product: {}'.format(prod)
                logger.error(err_st)
                errors.append(MonitorException(code=2, reason=err_st))
                
        if len(errors) > 0:
            ws_id = stream_monitor('worker', step='category', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
            for error in errors:
                stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=error.code, reason=str(error.reason))
        else:
            stream_monitor('worker', step='category', value=1, ms_id=params['ms_id'], store_id=params['store_id'])

    except Exception as e:
        err_st = "Unexpected error in crawl_cat: {}".format(e)
        ws_id = stream_monitor('worker', step='category', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=err_st)
        logger.error(err_st)
    return prods_ls



def process_prod(raw_prod, params):
    prod_cl = {}
    ws_id = stream_monitor('worker', step=params.get('route_key'), value=1, ms_id=params['ms_id'], store_id=params['store_id'])
    try:
        prod_cl = {
            'route_key' : params['route_key'],
            'retailer' : params['retailer_key'],
            'name' : raw_prod.get('name'),
            'id' :  raw_prod.get('product_id'),
            'url' : url_product.format(raw_prod.get('id')),
            'gtin' : raw_prod.get('ean'),
            'date' : str(datetime.datetime.utcnow()),
            'description' : raw_prod.get('description'),
            'brand' : raw_prod.get('trademark'),
            'provider' : '',
            'ingredients' : [],
            'images' : [
                url_image.format(raw_prod.get('image'))
            ],
            'raw_attributes' : [
                {
                    'key'  : 'content',
                    'value': raw_prod.get('quantity'),
                    'unit' : raw_prod.get('unit_type')
                }
            ],
            'raw_ingredients' : '',
            'price' : float(raw_prod.get('price')) if raw_prod.get('price') is not None else None,
            'price_original' : float(raw_prod.get('real_price')) if raw_prod.get('real_price') is not None else None,
            'discount' : float(raw_prod.get('discount')) if raw_prod.get('discount') is not None else None,
            'promo' : '',
            'location' : {
                'store' : [
                    params['store_uuid']
                ]
            }
        }
        # logger.debug(prod_cl)
    except Exception as e:
        err_st = "Unexpected error in process_prod: {}".format(e)
        ws_id = stream_monitor('worker', step=params.get('route_key'), value=1, ms_id=params['ms_id'], store_id=params['store_id'])
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=err_st)
        logger.error(err_st)
    return prod_cl
